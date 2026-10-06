                    case .int:
                        guard let newValue = newValue as? [Int], let existingValue = existingValue as? RealmSwift.MutableSet<Int> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .string:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<String> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.MutableSet<Bool> else { return true }
                        return Set(newValue) != Set(existingValue)

                    case .string:
                        guard let newValue = newValue as? [String] else {
                            return true
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<String> {
                            return newValue != Array(existingValue)
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<URL> {
                            return newValue
                                != existingValue.map(\.absoluteString)
                        }
                        return true
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.List<Bool> else { return true }
                        return newValue != Array(existingValue)

                        guard let newValue = newValue as? Int, let existingValue = existingValue as? Int else { return true }
                        return newValue != existingValue
                    case .string:
                        guard let newValue = newValue as? String, let existingValue = existingValue as? String else { return true }
                        return newValue != existingValue
                    case .bool:
                        guard let newValue = newValue as? Bool, let existingValue = existingValue as? Bool else { return true }
                        return newValue != existingValue

            case .string:
                let value = try requireArray(String.self, expected: "an array of strings")
                var set = Set<String>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .bool:
                let value = try requireArray(Bool.self, expected: "an array of booleans")
                var set = Set<Bool>()
