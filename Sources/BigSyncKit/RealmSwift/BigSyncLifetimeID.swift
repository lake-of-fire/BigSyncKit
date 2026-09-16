import Foundation

/// A reset's ordering belongs to the reset, not an Article title's modification
/// clock. Allocate once per logical reset and store the same ID on every member
/// of its bundle in the writer's transaction. Counter order follows observed
/// resets; simultaneous successors are arbitrated by nonce, not wall time.
/// Legacy opaque IDs are generation zero. No global clock, history, or new
/// CloudKit field is needed, but all reset writers must adopt this convention.
public enum BigSyncLifetimeID {
    private static let prefix = "bsk1:"

    public static func next(after predecessor: String?, nonce: UUID = UUID()) throws -> String {
        let previous = try components(predecessor)
        guard previous.generation < UInt64.max else { throw BigSyncRecordRebaseError.lifetimeOverflow }
        let digits = String(previous.generation + 1, radix: 16)
        return prefix + String(repeating: "0", count: 16 - digits.count) + digits
            + ":" + nonce.uuidString.lowercased()
    }

    public static func validate(_ identifier: String?) throws {
        _ = try components(identifier)
    }

    /// nil means both inputs are unversioned: the caller uses its legacy
    /// base-aware policy, never pretending random UUID order is causality.
    static func prefersIncoming(local: String?, incoming: String?) throws -> Bool? {
        let left = try components(local), right = try components(incoming)
        guard left.generation != 0 || right.generation != 0 else { return nil }
        if left.generation != right.generation { return right.generation > left.generation }
        return right.nonce > left.nonce
    }

    private static func components(_ value: String?) throws -> (generation: UInt64, nonce: String) {
        guard let value else { return (0, "") }
        guard value.hasPrefix("bsk") else { return (0, value) }
        let parts = value.split(separator: ":", omittingEmptySubsequences: false)
        guard value.hasPrefix(prefix), parts.count == 3, parts[1].count == 16,
              parts[1].allSatisfy({ ("0"..."9").contains(String($0)) || ("a"..."f").contains(String($0)) }),
              let generation = UInt64(parts[1], radix: 16), generation > 0,
              let nonce = UUID(uuidString: String(parts[2])),
              nonce.uuidString.lowercased() == parts[2] else {
            throw BigSyncRecordRebaseError.invalidLifetime
        }
        return (generation, String(parts[2]))
    }
}
