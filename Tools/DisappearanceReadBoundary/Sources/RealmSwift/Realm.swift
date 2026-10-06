// Portable test collaborator only; this is not the native SDK or adapter implementation.
import Foundation

// Explicit MVCC collaborator, not Realm: a write has separate provisional
// rows; freeze clones committed rows, including when a transaction is open.
// No SDK, disk durability, thread-confinement or queued admission is simulated.
open class Object {
    public required init() {}
    open var rowID: String { "" }
    public weak var realm: Realm?
    open class func className() -> String { String(describing: self) }
    open func copyRow() -> Object { type(of: self).init() }
}
public enum RealmFixtureError: Error { case ownerHeld, frozenWrite }
public final class Realm: @unchecked Sendable {
    public enum UpdatePolicy { case modified }
    public var isInWriteTransaction = false
    public private(set) var isFrozen = false
    public private(set) var refreshCount = 0
    public private(set) var freezeCount = 0
    public var onRefresh: (() -> Void)?
    private var committed: [String: Object] = [:]
    private var provisional: [String: Object] = [:]
    public init() {}
    private func key(_ type: Object.Type, _ id: String) -> String { type.className() + "|" + id }
    private func clones(_ input: [String: Object]) -> [String: Object] { input.mapValues { $0.copyRow() } }
    public func object<T: Object>(ofType type: T.Type, forPrimaryKey id: String) -> T? {
        let row = (isInWriteTransaction ? provisional : committed)[key(type, id)] as? T
        row?.realm = self
        return row
    }
    public func add(_ row: Object, update: UpdatePolicy = .modified) {
        precondition(isInWriteTransaction && !isFrozen)
        row.realm = self
        provisional[key(type(of: row), row.rowID)] = row
    }
    public func delete(_ row: Object) {
        precondition(isInWriteTransaction && !isFrozen)
        provisional.removeValue(forKey: key(type(of: row), row.rowID))
    }
    public func beginWrite() throws {
        guard !isFrozen else { throw RealmFixtureError.frozenWrite }
        guard !isInWriteTransaction else { throw RealmFixtureError.ownerHeld }
        provisional = clones(committed)
        isInWriteTransaction = true
    }
    public func commitWrite() {
        precondition(isInWriteTransaction)
        committed = clones(provisional); provisional = [:]; isInWriteTransaction = false
    }
    public func cancelWrite() {
        precondition(isInWriteTransaction)
        provisional = [:]; isInWriteTransaction = false
    }
    @discardableResult
    public func refresh() -> Bool {
        precondition(!isFrozen)
        refreshCount += 1
        let callback = onRefresh; onRefresh = nil; callback?()
        return !isInWriteTransaction
    }
    public func freeze() -> Realm {
        freezeCount += 1
        if isFrozen { return self }
        let result = Realm(); result.committed = clones(committed); result.isFrozen = true
        return result
    }
    public func write(_ operation: () throws -> Void) throws {
        try beginWrite()
        do { try operation(); commitWrite() } catch { cancelWrite(); throw error }
    }
}
