                    case .int:
                        guard let newValue = newValue as? [Int], let existingValue = existingValue as? RealmSwift.MutableSet<Int> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .string:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<String> else { return true }
                        return !BigSyncStringIdentity.unorderedValuesEqual(newValue, existingValue)
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.MutableSet<Bool> else { return true }
                        return Set(newValue) != Set(existingValue)

                    case .string:
                        guard let newValue = newValue as? [String] else {
                            return true
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<String> {
                            return !BigSyncStringIdentity.orderedValuesEqual(newValue, existingValue)
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<URL> {
                            return !BigSyncStringIdentity.orderedValuesEqual(
                                newValue, existingValue.lazy.map(\.absoluteString)
                            )
                        }
                        return true
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.List<Bool> else { return true }
                        return newValue != Array(existingValue)

                        guard let newValue = newValue as? Int, let existingValue = existingValue as? Int else { return true }
                        return newValue != existingValue
                    case .string:
                        guard let newValue = newValue as? String, let existingValue = existingValue as? String else { return true }
                        return !BigSyncStringIdentity.equal(newValue, existingValue)
                    case .bool:
                        guard let newValue = newValue as? Bool, let existingValue = existingValue as? Bool else { return true }
                        return newValue != existingValue

            case .string:
                let value = try requireArray(String.self, expected: "an array of strings")
                // Let Realm deduplicate the validated array by stored identity.
                // Swift Set<String> would first collapse byte-distinct Unicode
                // spellings. The common assignment below checks cancellation.
                recordValue = value
            case .bool:
                let value = try requireArray(Bool.self, expected: "an array of booleans")
                var set = Set<Bool>()
