#if DEBUG
import CloudKit
import Foundation
import RealmSwift

/// Explicit development tooling. Samples never enter a target Realm or the
/// synchronizer's zone, and include optional fields even when no local row has
/// ever populated them. Ordinary Debug launches do not seed application data.
public enum BigSyncDevelopmentSchema {
    public enum SchemaError: Error {
        case unsupportedProperty(String)
        case unkeyedType(String)
    }

    public static func records(
        objectTypes: [Object.Type],
        excludedClassNames: Set<String>,
        zoneID: CKRecordZone.ID,
        assetURL: URL
    ) throws -> [CKRecord] {
        let excluded = excludedClassNames
            .union([BigSyncPendingMutation.className()])
            .union(BigSyncLocalRecordEvidence.objectTypes.map { $0.className() })
        var seen = Set<String>()
        return try objectTypes.sorted { $0.className() < $1.className() }.compactMap { type in
            let name = type.className()
            guard !excluded.contains(name), seen.insert(name).inserted else { return nil }
            let object = type.init()
            guard let primary = object.objectSchema.primaryKeyProperty else {
                throw SchemaError.unkeyedType(name)
            }
            let skipped = (object as? SyncSkippablePropertiesModel)?.skipSyncingProperties() ?? []
            let stringIntegers = (type as? BigSyncStringEncodedIntegerModel.Type)?
                .bigSyncStringEncodedIntegerPropertyNames ?? []
            for key in stringIntegers {
                guard let property = object.objectSchema.properties.first(where: { $0.name == key }),
                      property.type == .int,
                      !property.isArray, !property.isSet, !property.isMap else {
                    throw SchemaError.unsupportedProperty("\(name).\(key)")
                }
            }
            let record = CKRecord(recordType: name,
                recordID: CKRecord.ID(recordName: "Schema.\(name)", zoneID: zoneID))
            for property in object.objectSchema.properties {
                guard property.name != primary.name, property.type != .linkingObjects,
                      !skipped.contains(property.name) else { continue }
                let key = property.name
                // The real encoders require nonoptional scalar collection
                // elements. Seeding one nonnil value would hide that mismatch.
                if property.type != .object, property.isOptional,
                   property.isMap || property.isArray || property.isSet {
                    throw SchemaError.unsupportedProperty("\(name).\(key)")
                }
                let sample: CKRecordValue
                if property.isMap {
                    switch property.type {
                    case .string, .UUID, .int, .bool, .float, .double, .date, .data: break
                    default: throw SchemaError.unsupportedProperty("\(name).\(key)")
                    }
                    // Both adapter encoders transport maps as binary plists.
                    sample = try PropertyListSerialization.data(
                        fromPropertyList: ["schema": "sample"], format: .binary, options: 0) as CKRecordValue
                } else {
                    switch property.type {
                    case .string: sample = "schema-sample" as CKRecordValue
                    case .UUID: sample = "00000000-0000-0000-0000-000000000042" as CKRecordValue
                    case .object:
                        // BigSync relationships contain record names, not
                        // CKRecord.Reference values (including collections).
                        sample = "\(property.objectClassName ?? name).schema-sample" as CKRecordValue
                    case .int:
                        sample = stringIntegers.contains(key)
                            ? BigSyncStringEncodedIntegerCodec.encode(42) as CKRecordValue
                            : NSNumber(value: Int64(42))
                    case .bool: sample = NSNumber(value: false)
                    case .float, .double: sample = NSNumber(value: 0.5)
                    case .date: sample = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
                    case .data:
                        // Scalar Data is a CKAsset; Data collections are bytes.
                        sample = property.isArray || property.isSet
                            ? Data([42]) as CKRecordValue : CKAsset(fileURL: assetURL)
                    default: throw SchemaError.unsupportedProperty("\(name).\(key)")
                    }
                    if property.isArray || property.isSet {
                        record[key] = [sample] as CKRecordValue
                        continue
                    }
                }
                record[key] = sample
            }
            record[cloudKitSynchronizerDeviceUUIDKey] = "schema-initializer" as CKRecordValue
            record[cloudKitSynchronizerModelCompatibilityVersionKey] = NSNumber(value: Int64(42))
            return record
        }
    }
}
#endif
