                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.MutableSet<Date> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<UUID> else { return true }
                        return Set(newValue.map { UUID(uuidString: $0) })
                            != Set(existingValue.map { Optional($0) })
                    default:
                        break
                    }

                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.List<Date> else { return true }
                        return newValue != Array(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.List<UUID> else { return true }
                        return !newValue.lazy.map { UUID(uuidString: $0) }.elementsEqual(
                            existingValue.lazy.map { Optional($0) }
                        )
                    default:
                        break
                    }

                } else if property.isMap {
                    guard let result = decodedCloudKitMap(newValue) else {
                        logger.warning("QSCloudKitSynchronizer >> Found unexpected property value: \(newValue)")
                        return true
                    }
                    switch property.type {
                    case .int:
                        guard let newValue = result as? [String: Int], let existingValue = existingValue as? RealmSwift.Map<String, Int> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    case .string:
                        guard let newValue = result as? [String: String], let existingValue = existingValue as? RealmSwift.Map<String, String> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }
                        )
                    case .bool:
                        guard let newValue = result as? [String: Bool], let existingValue = existingValue as? RealmSwift.Map<String, Bool> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    case .float:
                        guard let newValue = result as? [String: Float], let existingValue = existingValue as? RealmSwift.Map<String, Float> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    case .double:
                        guard let newValue = result as? [String: Double], let existingValue = existingValue as? RealmSwift.Map<String, Double> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    case .date:
                        guard let newValue = result as? [String: Date], let existingValue = existingValue as? RealmSwift.Map<String, Date> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    case .UUID:
                        guard let newValue = result as? [String: String], let existingValue = existingValue as? RealmSwift.Map<String, UUID> else { return true }
                        // The existing map codec transports UUID values as
                        // strings. Compare decoded identity, as for scalars.
                        // An invalid UUID stays nil and cannot match a value.
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue.lazy.map { (key: $0.key, value: UUID(uuidString: $0.value)) },
                            existingValue.lazy.map { (key: $0.key, value: Optional($0.value)) }, by: ==
                        )
                    case .data:
                        guard let newValue = result as? [String: Data], let existingValue = existingValue as? RealmSwift.Map<String, Data> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }, by: ==
                        )
                    default:
                        break
                    }
                } else {
                    switch property.type {
