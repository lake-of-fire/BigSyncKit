        case .reservedPrefix:
            return "CloudKit record names must not start with an underscore."
        }
    }
}

/// A downloaded candidate became stale without a durable local generation to
/// replace it. The page must be replayed; this is not a semantic quarantine.
struct RealmSwiftInboundTargetChangedError: Error, Equatable, Sendable {
    let recordName: String
}

enum RealmSwiftAdapterError: Error, LocalizedError {
    case setupUnavailable
    case malformedRecordIdentifier(recordName: String, entityType: String)
    case duplicateTrackedEntityType(entityType: String)
            appendPendingRelationship(
                name: key,
                syncedEntityID: syncedEntityIdentifier,
                targetIdentifiers: targetIdentifiers,
                record: record,
                to: &pendingRelationships
            )
        } else if property.type == .UUID {
            if let uuidString = record.value(forKey: key) as? String,
               let uuid = UUID(uuidString: uuidString) {
                try Task.checkCancellation()
                object.setValue(uuid, forKey: key)
            } else if value != nil {
                throw malformed("a UUID string")
            } else if property.isOptional {
                try Task.checkCancellation()
                object.setValue(nil, forKey: key)
            }
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
               let data = NSData(contentsOf: fileURL) {
                try Task.checkCancellation()
                object.setValue(data, forKey: key)
            } else {
                throw malformed("a readable CloudKit asset")
            }
                if let recordProcessingDelegate = recordProcessingDelegate,
                   !recordProcessingDelegate.shouldProcessPropertyBeforeUpload(propertyName: property.name, object: object, record: record) {
                    continue
                }

                if property.type == PropertyType.object,
                   !property.isArray, !property.isSet, !property.isMap {
                    if let target = object[property.name] as? Object {
                        let targetPrimaryKey = (type(of: target).primaryKey() ?? target.objectSchema.primaryKeyProperty?.name)!
                        let targetIdentifier = Self.getTargetObjectStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
                        let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                        try Self.validateCloudKitRecordName(referenceIdentifier)
                        let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                        record[property.name] = recordID.recordName as CKRecordValue
                    } else {
                        record[property.name] = nil
                    }
                } else if property.isSet {
                    let value = object[property.name]
                    switch property.type {
                    case .object:
                        /// We may get MutableSet<Cat> here
                    if let mapValue {
                        // Match Realm's other collection encodings: an empty
                        // collection is represented by an absent CloudKit
                        // field. Comparison and audit paths normalize a remote
                        // empty map to the same semantic value.
                        record[property.name] = mapValue.isEmpty
                            ? nil
                            : try encodedCloudKitMap(mapValue) as CKRecordValue
                    } else {
                        throw RealmSwiftRemoteRecordDecodingError.malformedField(
                            recordName: syncedEntity.identifier,
                            propertyName: property.name,
                            expected: "a supported scalar Realm map for upload"
                        )
                    }
                } else if property.isArray {
                    // Array handling forked from IceCream: https://github.com/caiyue1993/IceCream/blob/b29dfe81e41cc929c8191c3266189a7070cb5bc5/IceCream/Classes/CKRecordConvertible.swift
                    let value = object[property.name]
                    switch property.type {
                    case .object:
                                        if !selectionStillMatches,
                                           replacementDisposition != .preferIncomingRecord,
                                           replacementDisposition != .preferExistingObject {
                                            if let currentMutationGeneration {
                                                preservedDispositionsByRecordName[
                                                    candidate.syncedEntityID
                                                ] = .preservedPendingLocal(
                                                    generation:
                                                        currentMutationGeneration
                                                )
                                            }
                                            guard currentMutationGeneration != nil else {
                                                // Derived metadata is not journaled user intent.
                                                // Fail the page rather than acknowledge an
                                                // unapplied server payload as unchanged.
                                                throw RealmSwiftInboundTargetChangedError(
                                                    recordName: candidate.syncedEntityID
                                                )
                                            }
                                            logger.info(
                                                "QSCloudKitSynchronizer >> Skipped downloaded record after a newer local mutation: \(candidate.syncedEntityID)"
                                            )
                                            continue
                                        }
                                        if currentMutationGeneration != nil,
                                           replacementDisposition != .preferIncomingRecord {
                                            // The durable local journal is the
                                            // authority for user intent. Keep
                                            // the target values untouched while
                                            // still allowing the later tracking
