import CloudKit
import Foundation
import RealmSwift

/// The meaning of an absent CloudKit field in an adopted comparison contract.
/// This is a representation policy, not a merge policy or a second value codec.
public enum BigSyncIncomingFieldOmission: Sendable, Equatable {
    /// The complete incoming representation must contain this field.
    case required
    /// Absence clears an optional, list, set or map, using the transport codec.
    case nilOrEmpty
    /// A named, released compatibility interpretation. Never read an initializer
    /// to supply this value: initializer changes must not reinterpret evidence.
    case compatibilityDefault(BigSyncIncomingDefaultValue)
}

/// Deliberately small deterministic scalar vocabulary. Collections can be empty
/// on omission, but cannot acquire synthesized members. Dates are explicit
/// milliseconds, not a closure capable of manufacturing Date() on each decode.
public enum BigSyncIncomingDefaultValue: Sendable, Equatable {
    case integer(Int64)
    case boolean(Bool)
    case string(String)
    case floatingPoint(Double)
    case dateMillisecondsSince1970(Int64)
    case uuid(UUID)
    case data(Data)

    fileprivate func isSupported(by property: Property) -> Bool {
        guard !property.isArray, !property.isSet, !property.isMap else { return false }
        switch self {
        case .integer: return property.type == .int
        case .boolean: return property.type == .bool
        case .string: return property.type == .string
        case let .floatingPoint(value):
            return value.isFinite && (property.type == .double
                || (property.type == .float && Float(value).isFinite))
        case .dateMillisecondsSince1970: return property.type == .date
        case .uuid: return property.type == .UUID
        case .data: return property.type == .data
        }
    }

    /// These are model values. The existing record encoder still determines
    /// their wire representation (including string-encoded integers/assets).
    fileprivate var modelValue: Any {
        switch self {
        case let .integer(value): return NSNumber(value: value)
        case let .boolean(value): return NSNumber(value: value)
        case let .string(value): return value
        case let .floatingPoint(value): return NSNumber(value: value)
        case let .dateMillisecondsSince1970(value):
            return Date(timeIntervalSince1970: Double(value) / 1_000)
        case let .uuid(value): return value
        case let .data(value): return value
        }
    }

    fileprivate var signatureParts: [String] {
        switch self {
        case let .integer(value): return ["integer", String(value)]
        case let .boolean(value): return ["boolean", value ? "true" : "false"]
        case let .string(value): return ["string", value]
        case let .floatingPoint(value):
            return ["floating-point-bits", String(value.bitPattern)]
        case let .dateMillisecondsSince1970(value):
            return ["unix-milliseconds", String(value)]
        case let .uuid(value): return ["uuid", value.uuidString.lowercased()]
        case let .data(value): return ["data", value.base64EncodedString()]
        }
    }
}

public struct BigSyncIncomingRepresentationPolicy: Sendable, Equatable {
    /// Stable product/format name. Increment version when an omission changes
    /// meaning, even when the Realm schema has not changed.
    public let identity: String
    public let version: Int
    public let fields: [String: BigSyncIncomingFieldOmission]

    public init(
        identity: String,
        version: Int = 1,
        fields: [String: BigSyncIncomingFieldOmission] = [:]
    ) {
        self.identity = identity
        self.version = version
        self.fields = fields
    }

    /// Unlisted optional/collection fields mean nil/empty; every other unlisted
    /// field is required. This default never consults a model initializer.
    public static let strict = Self(identity: "bigsync-complete-record", version: 1)

    func omission(for property: Property) -> BigSyncIncomingFieldOmission {
        fields[property.name] ?? (property.isOptional || property.isArray
            || property.isSet || property.isMap ? .nilOrEmpty : .required)
    }

    static func transportedProperties(of object: Object) -> [Property] {
        let primaryKey = object.objectSchema.primaryKeyProperty?.name
        let skipped = (object as? SyncSkippablePropertiesModel)?.skipSyncingProperties() ?? []
        return object.objectSchema.properties.filter {
            $0.name != primaryKey && $0.type != .linkingObjects && !skipped.contains($0.name)
        }
    }

    func validate(for object: Object) throws {
        let properties = Self.transportedProperties(of: object)
        guard !identity.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              version > 0,
              Set(fields.keys).isSubset(of: Set(properties.map(\.name))) else {
            throw BigSyncRecordContractError.invalidDeclaration(object.objectSchema.className)
        }
        for property in properties {
            let valid: Bool
            switch omission(for: property) {
            case .required: valid = true
            case .nilOrEmpty:
                valid = property.isOptional || property.isArray || property.isSet || property.isMap
            case let .compatibilityDefault(value): valid = value.isSupported(by: property)
            }
            guard valid else {
                throw BigSyncRecordContractError.invalidDeclaration(
                    object.objectSchema.className + "." + property.name)
            }
        }
    }

    /// Coverage and presence are distinct checks. Validate presence before
    /// lifecycle/immutable skips too, without decoding or manufacturing values.
    func validatePresence(in record: CKRecord, for object: Object) throws {
        try validate(for: object)
        for property in Self.transportedProperties(of: object)
            where record[property.name] == nil {
            if case .required = omission(for: property) {
                throw BigSyncIncomingRepresentationError.missingRequiredField(
                    recordType: object.objectSchema.className, field: property.name)
            }
        }
    }

    /// Called exactly once for each absent field while the adapter decodes the
    /// comparison object. Nil/empty is left to its existing applyChange codec.
    /// Returns true only when a declared scalar default was installed.
    func applyOmission(to object: Object, property: Property) throws -> Bool {
        switch omission(for: property) {
        case .required:
            throw BigSyncIncomingRepresentationError.missingRequiredField(
                recordType: object.objectSchema.className, field: property.name)
        case .nilOrEmpty: return false
        case let .compatibilityDefault(value):
            guard value.isSupported(by: property) else {
                throw BigSyncRecordContractError.invalidDeclaration(
                    object.objectSchema.className + "." + property.name)
            }
            object.setValue(value.modelValue, forKey: property.name)
            return true
        }
    }

    var signatureParts: [String] {
        var parts = ["incoming-representation", identity, String(version), String(fields.count)]
        for name in fields.keys.sorted() {
            let rule: [String]
            switch fields[name]! {
            case .required: rule = ["required"]
            case .nilOrEmpty: rule = ["nil-or-empty"]
            case let .compatibilityDefault(value):
                rule = ["compatibility-default"] + value.signatureParts
            }
            parts += [name, String(rule.count)] + rule
        }
        return parts
    }
}

public enum BigSyncIncomingRepresentationError: Error, Equatable,
    BigSyncInboundSemanticValidationFailure {
    case missingRequiredField(recordType: String, field: String)
    case actualValueMismatch(recordName: String)

    public var bigSyncValidationCode: String {
        switch self {
        case .missingRequiredField: return "comparison-missing-required-field"
        case .actualValueMismatch: return "comparison-actual-value-mismatch"
        }
    }
}
