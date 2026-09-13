    case unknownInboundEntityType(String)
    case malformedStringEncodedInteger(
        entityType: String,
        propertyName: String
    )

    var errorDescription: String? {
        switch self {
        case .setupUnavailable:
            return "The Realm adapter has not completed setup."
        case let .malformedRecordIdentifier(recordName, entityType):
            return "Record name \(recordName) does not contain a valid \(entityType) identifier."
        case let .duplicateTrackedEntityType(entityType):
            return "Tracked Realm entity type \(entityType) is present in more than one target Realm."
        case let .missingChangeMetadataConformance(entityType):
            return "Could not durably encode CloudKit system fields for \(recordName)."
        case let .accountScopeUnavailable(entityType):
            return "No validated CloudKit account scope is active for \(entityType)."
        case let .accountScopeMismatch(entityType, expected, actual):
            let received = actual ?? "nil"
            return "CloudKit account scope mismatch for \(entityType); expected \(expected), received \(received)."
        case let .malformedStringEncodedInteger(entityType, propertyName):
            return "Realm integer property \(entityType).\(propertyName) could not be encoded for CloudKit."
        case .unknownInboundEntityType(let entityType):
            return "CloudKit delivered unknown Realm entity type \(entityType)."
        }
    }
}

enum RealmSwiftRemoteRecordDecodingError: Error, LocalizedError {
    case malformedField(recordName: String, propertyName: String, expected: String)

    var errorDescription: String? {
        switch self {
        case let .malformedField(recordName, propertyName, expected):
        }
        func requireArray<Element>(
            _ type: Element.Type,
            expected: String
        ) throws -> [Element] {
            guard let result = value as? [Element] else {
                throw malformed(expected)
            }
            return result
        }
        if (property.isSet || property.isArray || property.isMap),
           !record.allKeys().contains(key) {
            // Full zone-change fetches use desiredKeys == nil, so an absent
            // collection field is the CloudKit representation of an empty
            // collection. Realm collections cannot be assigned nil.
            if property.type == .object {
                guard property.objectClassName != nil else {
                    throw malformed("a Realm relationship type")
                }
                // Replace any older deferred relationship with an explicit

        //        let changedKeys = (syncedEntity.changedKeys ?? "").components(separatedBy: ",")

        //        var parentKey: String?
        //        if let childObject = object as? ParentKey {
        //            parentKey = type(of: childObject).parentKey()
        //        }

        for property in object.objectSchema.properties {
            guard !cancelSync else { throw CancellationError() }

            //            if object.objectSchema.className == "HistoryRecord" && property.name == "content" && record.id == "6657C67E-95EC-479B-B5F5-9F7F44EAB1C5" {
            //                debugPrint(property)
            //            }
            if includeAllProperties || entityState == SyncedEntityState.new.rawValue || entityState == SyncedEntityState.changed.rawValue {
                if skippedKeys.contains(property.name) {
                    let defaultValue = defaultObject?[property.name]
                    if let ckValue = defaultValue as? CKRecordValue {
                        record[property.name] = ckValue
                    } else {
                        record[property.name] = nil
                    }
                    continue
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
                        /// The item cannot be casted as MutableSet<Object>
                        /// It can be casted at a low-level type `SetBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let set = value as? RLMSwiftCollectionBase else { break }
                        var referenceArray = [String]()
                        let wrappedSet = set._rlmCollection
                        for index in 0..<wrappedSet.count {
                            guard let object = wrappedSet[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let set = value as? MutableSet<Int> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        guard let set = value as? MutableSet<String> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let set = value as? MutableSet<Bool> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let set = value as? MutableSet<Float> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let set = value as? MutableSet<Double> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let set = value as? MutableSet<Data> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let set = value as? MutableSet<Date> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let set = value as? MutableSet<UUID> else { break }
                        let array = Array(set.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    default:
                        // Other inner types of Set is not supported yet
                        logger.warning("Warning: Unsupported recordToUpload set property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if property.isMap {
                    // CloudKit rejects nested arrays and has no native dictionary
                    // value. A binary property list preserves supported Realm
                    // scalar map values in one CloudKit Data field.
                    let mapValue: [String: Any]?
                    switch property.type {
                    case .int:
                        mapValue = (object[property.name] as? Map<String, Int>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                    }
                } else if property.isArray {
                    // Array handling forked from IceCream: https://github.com/caiyue1993/IceCream/blob/b29dfe81e41cc929c8191c3266189a7070cb5bc5/IceCream/Classes/CKRecordConvertible.swift
                    let value = object[property.name]
                    switch property.type {
                    case .object:
                        /// We may get List<Cat> here
                        /// The item cannot be casted as List<Object>
                        /// It can be casted at a low-level type `ListBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let list = value as? RLMSwiftCollectionBase else { break }
                        var referenceArray = [String]()
                        let wrappedArray = list._rlmCollection
                        for index in 0..<wrappedArray.count {
                            guard let object = wrappedArray[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let list = value as? List<Int> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        let array: [String]
                        if let list = value as? List<String> {
                            array = Array(list)
                        } else if let list = value as? List<URL> {
                            array = list.map(\.absoluteString)
                        } else {
                            break
                        }
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let list = value as? List<Bool> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let list = value as? List<Float> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let list = value as? List<Double> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let list = value as? List<Data> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let list = value as? List<Date> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let list = value as? List<UUID> else { break }
                        let array = Array(list.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    default:
                        // Other inner types of List is not supported yet
                        logger.warning("Warning: Unsupported recordToUpload array property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if (
                    property.type != PropertyType.linkingObjects &&
                    !(property.name == (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!)
                ) {
                    let value = object[property.name]
                    if property.type == PropertyType.data,
                       let data = value as? Data,
                       !forceDataTypeInsteadOfAsset {
                        let fileURL = try self.persistentAssetManager.store(
                        // semantic validators to reject the downloaded record.
                        // Normalize from the authoritative schema type before
                        // handing the value to CKRecord.
                        record[property.name] = NSNumber(
                            value: number.int64Value
                        )
                    } else if property.type == PropertyType.UUID, let uuid = value as? UUID {
                        record[property.name] = uuid.uuidString as CKRecordValue
                    } else if let recordValue = value as? CKRecordValue {
                        record[property.name] = recordValue
                    }
                }

            }
        }
