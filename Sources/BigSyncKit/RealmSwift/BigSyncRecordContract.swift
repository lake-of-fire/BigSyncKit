import CryptoKit
import Foundation
import RealmSwift

/// A model's transport lifecycle, not its visibility. A retained tombstone is
/// a versioned save with isDeleted == true; only an explicit erasure operation
/// outside ordinary synchronization may physically remove that record.
public enum BigSyncRecordDeletionBehavior: String, Sendable {
    case physical
    case retained
}

/// One immutable description shared by authoring, inbound comparison, upload
/// preparation, receipt acknowledgement and cleanup. No application merge
/// callbacks or additional clocks participate in this contract.
public struct BigSyncRecordContract: Sendable, Equatable {
    public let policy: BigSyncRecordRebasePolicy
    public let deletion: BigSyncRecordDeletionBehavior
    public let semanticMetadataFields: Set<String>
    public let atomicFieldGroups: [Set<String>]
    public let expectedFields: Set<String>?
    public let preserveConflictingFields: Set<String>
    public let incomingRepresentation: BigSyncIncomingRepresentationPolicy

    public init(
        policy: BigSyncRecordRebasePolicy,
        deletion: BigSyncRecordDeletionBehavior = .physical,
        semanticMetadataFields: Set<String> = [],
        atomicFieldGroups: [Set<String>] = [],
        expectedFields: Set<String>? = nil,
        preserveConflictingFields: Set<String> = [],
        incomingRepresentation: BigSyncIncomingRepresentationPolicy = .strict
    ) {
        self.policy = policy
        self.deletion = deletion
        self.semanticMetadataFields = semanticMetadataFields
        self.atomicFieldGroups = atomicFieldGroups
        self.expectedFields = expectedFields
        self.preserveConflictingFields = preserveConflictingFields
        self.incomingRepresentation = incomingRepresentation
    }
}

public protocol BigSyncRecordContractProviding: BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordContract: BigSyncRecordContract { get }
}

public extension BigSyncRecordContractProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        bigSyncRecordContract.policy
    }
}

/// All local evidence is registered and excluded together. The target journal
/// remains the only source of latest upload work; these tables never upload.
public enum BigSyncLocalRecordEvidence {
    public static var objectTypes: [ObjectBase.Type] {
        [BigSyncRecordBaseline.self, BigSyncRecordSubmission.self,
         BigSyncRecordConflict.self]
    }

    static var classNames: [String] {
        [BigSyncPendingMutation.className()] + objectTypes.map { $0.className() }
    }
}

public enum BigSyncRecordContractError: Error, Equatable {
    case missingEvidenceSchema(String)
    case invalidDeclaration(String)
    case unclassifiedFields(String)
    case missingRetainedTarget(String)
    case unexpectedPhysicalDeletion(String)
    case evidenceCapacityExceeded
    case staleConflict
}

/// These predicates do not inspect a tracking state or a mutation generation.
/// Queue transitions cannot change a model's deletion representation.
enum BigSyncRecordLifecycle {
    static func retainsTombstone(_ type: Object.Type) -> Bool {
        if (type as? BigSyncRecordContractProviding.Type)?
            .bigSyncRecordContract.deletion == .retained { return true }
        guard type is BigSyncRetainsSyncedTombstone.Type else { return false }
        return (type.init() as? BigSyncRetainsSyncedTombstone)?.retainsSyncedTombstone == true
    }

    static func isPhysicalDeletion(_ object: Object) -> Bool {
        (object as? SoftDeletable)?.isDeleted == true
            && !retainsTombstone(type(of: object))
    }

    static func metadataFields(for object: Object) -> Set<String> {
        BigSyncRecordFingerprint.metadata.subtracting(
            (type(of: object) as? BigSyncRecordContractProviding.Type)?
                .bigSyncRecordContract.semanticMetadataFields ?? []
        )
    }
}

struct BigSyncCompiledRecordContract: Sendable {
    let declaration: BigSyncRecordContract
    let signature: String

    static func compile(_ object: Object) throws -> Self? {
        guard let provider = type(of: object) as? BigSyncRecordContractProviding.Type else {
            return nil
        }
        let contract = provider.bigSyncRecordContract
        let name = object.objectSchema.className
        let fields = Set(BigSyncRecordFingerprint.properties(of: object).map(\.name))
        guard contract.policy != .disabled,
              contract.semanticMetadataFields.isSubset(of: BigSyncRecordFingerprint.metadata),
              !contract.semanticMetadataFields.contains("modifiedAt"),
              !contract.semanticMetadataFields.contains("explicitlyModifiedAt"),
              contract.preserveConflictingFields.isSubset(of: fields) else {
            throw BigSyncRecordContractError.invalidDeclaration(name)
        }
        if let expected = contract.expectedFields, fields != expected {
            throw BigSyncRecordContractError.unclassifiedFields(name)
        }
        try contract.incomingRepresentation.validate(for: object)
        var grouped = Set<String>()
        for group in contract.atomicFieldGroups {
            guard !group.isEmpty, group.isSubset(of: fields), grouped.isDisjoint(with: group) else {
                throw BigSyncRecordContractError.invalidDeclaration(name)
            }
            grouped.formUnion(group)
        }
        if case let .lifetimeBundle(lifetime, independent) = contract.policy {
            guard fields.contains(lifetime), !independent.contains(lifetime),
                  independent.isSubset(of: fields),
                  contract.atomicFieldGroups.allSatisfy({ $0.isSubset(of: independent) }) else {
                throw BigSyncRecordContractError.invalidDeclaration(name)
            }
        } else if contract.deletion == .retained {
            // Reusable retained identities require an explicit lifetime order.
            throw BigSyncRecordContractError.invalidDeclaration(name)
        }
        if !contract.preserveConflictingFields.isEmpty {
            guard object.objectSchema.primaryKeyProperty?.type == .UUID,
                  contract.policy == .independentFields,
                  contract.deletion == .physical else {
                throw BigSyncRecordContractError.invalidDeclaration(name)
            }
        }
        var parts = ["bigsync-record-contract-v2", name, contract.deletion.rawValue]
        parts += contract.incomingRepresentation.signatureParts
        switch contract.policy {
        case .disabled: parts.append("disabled")
        case .atomicRecord: parts.append("atomic")
        case .independentFields: parts.append("independent")
        case let .lifetimeBundle(field, independent):
            parts += ["lifetime", field] + independent.sorted()
        }
        parts += ["semantic-metadata"] + contract.semanticMetadataFields.sorted()
        parts += ["preserve"] + contract.preserveConflictingFields.sorted()
        for group in contract.atomicFieldGroups.map({ $0.sorted() }).sorted(by: {
            $0.lexicographicallyPrecedes($1)
        }) { parts += ["group"] + group }
        let skipped = (object as? SyncSkippablePropertiesModel)?.skipSyncingProperties() ?? []
        let stringIntegers = (type(of: object) as? BigSyncStringEncodedIntegerModel.Type)?
            .bigSyncStringEncodedIntegerPropertyNames ?? []
        for property in object.objectSchema.properties.sorted(by: { $0.name < $1.name }) {
            parts += [property.name, String(describing: property.type),
                      String(property.isOptional), String(property.isArray),
                      String(property.isSet), String(property.isMap),
                      String(skipped.contains(property.name)),
                      String(stringIntegers.contains(property.name))]
        }
        var hash = SHA256()
        for part in parts {
            let bytes = Data(part.utf8)
            var size = UInt64(bytes.count).bigEndian
            withUnsafeBytes(of: &size) { hash.update(data: Data($0)) }
            hash.update(data: bytes)
        }
        return Self(declaration: contract,
                    signature: hash.finalize().map { String(format: "%02x", $0) }.joined())
    }
}

public extension BigSyncRecordContract {
    /// Validate declarations at canonical configuration creation, before any
    /// Realm is opened. This never installs an account or mutation identity.
    static func validate(configuration: Realm.Configuration) throws {
        guard let types = configuration.objectTypes else {
            throw BigSyncRecordContractError.missingEvidenceSchema("explicit objectTypes")
        }
        let names = Set(types.map { $0.className() })
        for type in types.compactMap({ $0 as? Object.Type })
            where type is BigSyncRecordContractProviding.Type {
            guard names.contains(BigSyncPendingMutation.className()),
                  BigSyncLocalRecordEvidence.objectTypes.allSatisfy({ names.contains($0.className()) }) else {
                throw BigSyncRecordContractError.missingEvidenceSchema(type.className())
            }
            let object = type.init()
            guard BigSyncRecordFingerprint.supports(object) else {
                throw BigSyncRecordContractError.invalidDeclaration(type.className())
            }
            _ = try BigSyncCompiledRecordContract.compile(object)
        }
    }
}
