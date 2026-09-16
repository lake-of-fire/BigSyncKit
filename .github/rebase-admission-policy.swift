    private func recordRebasePolicy(for object: Object) throws -> BigSyncRecordRebasePolicy {
        let declaration = type(of: object) as? BigSyncRecordRebasePolicyProviding.Type
        let declared = declaration?.bigSyncRecordRebasePolicy
        if declared == .disabled { return .disabled }
        guard BigSyncRecordFingerprint.supports(object) else {
            // Undeclared legacy models retain their original behavior. An
            // explicit declaration must not look enabled while silently using
            // whole-record replacement for an unsupported relationship/type.
            if declared != nil {
                let field = BigSyncRecordFingerprint.properties(of: object).first {
                    switch $0.type {
                    case .int, .bool, .float, .double, .string, .date, .data, .UUID: false
                    default: true
                    }
                }
                throw BigSyncRecordRebaseError.unsupportedField(field?.name ?? object.objectSchema.className)
            }
            return .disabled
        }
        if let policy = declared {
            // Semantic payloads cannot be assembled property-by-property.
            // Reject invalid declarations before the first baseline is stored,
            // not only after a pending local mutation happens to exercise it.
            if object is BigSyncInboundSemanticRecordValidating
                || object is BigSyncInboundSemanticReplacementValidating {
                guard case let .lifetimeBundle(_, fields) = policy, fields.isEmpty else {
                    throw BigSyncRecordRebaseError.invalidPolicy
                }
            }
            if case let .lifetimeBundle(field, independent) = policy {
                let properties = BigSyncRecordFingerprint.properties(of: object)
                guard let property = properties.first(where: { $0.name == field }),
                      !property.isArray, !property.isSet, !property.isMap,
                      property.type == .string || property.type == .UUID,
                      !independent.contains(field),
                      independent.isSubset(of: Set(properties.map(\.name))) else {
                    throw BigSyncRecordRebaseError.invalidPolicy
                }
            }
            return policy
        }
        if object is BigSyncAuthoritativeServerSnapshotModel
            || object is BigSyncInboundSemanticRecordValidating
            || object is BigSyncInboundSemanticReplacementValidating {
            return .disabled
        }
        // Unknown domain invariants must not be split into unrelated fields.
        return .atomicRecord
    }
