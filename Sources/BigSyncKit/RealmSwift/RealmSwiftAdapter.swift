        expected: String,
        actual: String?
    )
    case unknownInboundEntityType(String)
    case unsupportedUploadProperty(entityType: String, propertyName: String)
    case malformedStringEncodedInteger(
        entityType: String,
        propertyName: String
    )
        case let .malformedStringEncodedInteger(entityType, propertyName):
            return "Realm integer property \(entityType).\(propertyName) could not be encoded for CloudKit."
        case .unknownInboundEntityType(let entityType):
            return "CloudKit delivered unknown Realm entity type \(entityType)."
        case let .unsupportedUploadProperty(entityType, propertyName):
            return "Realm property \(entityType).\(propertyName) has no supported CloudKit upload encoding."
        }
    }
}

                throw malformed(expected)
            }
            return result
        }
        // Object maps have no keyed deferred-relationship representation.
        // Even an absent map must not become a to-one clear during replay.
        if property.isMap && property.type == .object {
            throw malformed("a supported scalar Realm map")
        }
        if (property.isSet || property.isArray || property.isMap),
           !record.allKeys().contains(key) {
            // Full zone-change fetches use desiredKeys == nil, so an absent
            // collection field is the CloudKit representation of an empty
        //        }

        for property in object.objectSchema.properties {
            guard !cancelSync else { throw CancellationError() }
            func unsupportedProperty() -> RealmSwiftAdapterError {
                .unsupportedUploadProperty(
                    entityType: object.objectSchema.className,
                    propertyName: property.name
                )
            }

            //            if object.objectSchema.className == "HistoryRecord" && property.name == "content" && record.id == "6657C67E-95EC-479B-B5F5-9F7F44EAB1C5" {
            //                debugPrint(property)
            //            }
                   !recordProcessingDelegate.shouldProcessPropertyBeforeUpload(propertyName: property.name, object: object, record: record) {
                    continue
                }

                // Backlinks are derived, not unsupported authored fields.
                // Explicit skips and delegate overrides above still take priority.
                guard property.type != .linkingObjects else { continue }

                if property.type == PropertyType.object,
                   !property.isArray, !property.isSet, !property.isMap {
                    if let value = object[property.name] {
                        guard let target = value as? Object,
                              let targetPrimaryKey = type(of: target).primaryKey()
                                ?? target.objectSchema.primaryKeyProperty?.name,
                              let targetEntityType = property.objectClassName else {
                            throw unsupportedProperty()
                        }
                        let targetIdentifier = Self.getTargetObjectStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
                        let referenceIdentifier = "\(targetEntityType).\(targetIdentifier)"
                        try Self.validateCloudKitRecordName(referenceIdentifier)
                        let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                        record[property.name] = recordID.recordName as CKRecordValue
                    } else {
                        /// We may get MutableSet<Cat> here
                        /// The item cannot be casted as MutableSet<Object>
                        /// It can be casted at a low-level type `SetBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let set = value as? RLMSwiftCollectionBase else { throw unsupportedProperty() }
                        var referenceArray = [String]()
                        let wrappedSet = set._rlmCollection
                        for index in 0..<wrappedSet.count {
                            guard let object = wrappedSet[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name),
                                  let targetEntityType = property.objectClassName else {
                                throw unsupportedProperty()
                            }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(targetEntityType).\(targetIdentifier)"
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let set = value as? MutableSet<Int> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        guard let set = value as? MutableSet<String> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let set = value as? MutableSet<Bool> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let set = value as? MutableSet<Float> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let set = value as? MutableSet<Double> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let set = value as? MutableSet<Data> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let set = value as? MutableSet<Date> else { throw unsupportedProperty() }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let set = value as? MutableSet<UUID> else { throw unsupportedProperty() }
                        let array = Array(set.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    default:
                        throw unsupportedProperty()
                    }
                } else if property.isMap {
                    // CloudKit rejects nested arrays and has no native dictionary
                    // value. A binary property list preserves supported Realm
                        /// We may get List<Cat> here
                        /// The item cannot be casted as List<Object>
                        /// It can be casted at a low-level type `ListBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let list = value as? RLMSwiftCollectionBase else { throw unsupportedProperty() }
                        var referenceArray = [String]()
                        let wrappedArray = list._rlmCollection
                        for index in 0..<wrappedArray.count {
                            guard let object = wrappedArray[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name),
                                  let targetEntityType = property.objectClassName else {
                                throw unsupportedProperty()
                            }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(targetEntityType).\(targetIdentifier)"
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let list = value as? List<Int> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        let array: [String]
                            array = Array(list)
                        } else if let list = value as? List<URL> {
                            array = list.map(\.absoluteString)
                        } else {
                            throw unsupportedProperty()
                        }
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let list = value as? List<Bool> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let list = value as? List<Float> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let list = value as? List<Double> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let list = value as? List<Data> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let list = value as? List<Date> else { throw unsupportedProperty() }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let list = value as? List<UUID> else { throw unsupportedProperty() }
                        let array = Array(list.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    default:
                        throw unsupportedProperty()
                    }
                } else if (
                    property.type != PropertyType.linkingObjects &&
                    !(property.name == (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!)
                    } else if property.type == PropertyType.UUID, let uuid = value as? UUID {
                        record[property.name] = uuid.uuidString as CKRecordValue
                    } else if let recordValue = value as? CKRecordValue {
                        record[property.name] = recordValue
                    } else {
                        // A complete journal generation must never acknowledge
                        // a non-nil value that was silently omitted from its record.
                        throw unsupportedProperty()
                    }
                }

            }
