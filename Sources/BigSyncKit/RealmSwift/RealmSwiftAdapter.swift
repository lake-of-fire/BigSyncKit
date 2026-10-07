                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.MutableSet<Date> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<UUID> else { return true }
                        return Set(newValue) != Set(Array(existingValue).map { $0.uuidString })
                    default:
                        break
                    }

                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.List<Date> else { return true }
                        return newValue != Array(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.List<UUID> else { return true }
                        return newValue != Array(existingValue).map { $0.uuidString }
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
                        return newValue != existingValue.reduce(into: [String: Int]()) { $0[$1.key] = $1.value }
                    case .string:
                        guard let newValue = result as? [String: String], let existingValue = existingValue as? RealmSwift.Map<String, String> else { return true }
                        return !BigSyncStringIdentity.mappedValuesEqual(
                            newValue, existingValue.lazy.map { (key: $0.key, value: $0.value) }
                        )
                    case .bool:
                        guard let newValue = result as? [String: Bool], let existingValue = existingValue as? RealmSwift.Map<String, Bool> else { return true }
                        return newValue != existingValue.reduce(into: [String: Bool]()) { $0[$1.key] = $1.value }
                    case .float:
                        guard let newValue = result as? [String: Float], let existingValue = existingValue as? RealmSwift.Map<String, Float> else { return true }
                        return newValue != existingValue.reduce(into: [String: Float]()) { $0[$1.key] = $1.value }
                    case .double:
                        guard let newValue = result as? [String: Double], let existingValue = existingValue as? RealmSwift.Map<String, Double> else { return true }
                        return newValue != existingValue.reduce(into: [String: Double]()) { $0[$1.key] = $1.value }
                    case .date:
                        guard let newValue = result as? [String: Date], let existingValue = existingValue as? RealmSwift.Map<String, Date> else { return true }
                        return newValue != existingValue.reduce(into: [String: Date]()) { $0[$1.key] = $1.value }
                    case .UUID:
                        guard let newValue = result as? [String: UUID], let existingValue = existingValue as? RealmSwift.Map<String, UUID> else { return true }
                        return newValue != existingValue.reduce(into: [String: UUID]()) { $0[$1.key] = $1.value }
                    case .data:
                        guard let newValue = result as? [String: Data], let existingValue = existingValue as? RealmSwift.Map<String, Data> else { return true }
                        return newValue != existingValue.reduce(into: [String: Data]()) { $0[$1.key] = $1.value }
                    default:
                        break
                    }
                } else {
                    switch property.type {
