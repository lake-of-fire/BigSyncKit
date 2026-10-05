import Foundation

// Explicit MVCC/collection collaborator only. No native SDK, scheduling,
// cross-file transaction, journal generation, disk, or durability simulation.
public struct Property { public let name: String; public init(_ name: String) { self.name = name } }
public struct ObjectSchema {
    public let className: String
    public let primaryKeyProperty: Property?
}
public struct Schema { public let objectSchema: [ObjectSchema] }
open class Object {
    public var values: [String: Any] = [:]
    public weak var realm: Realm?
    public required init() {}
    open class func className() -> String { String(describing: self) }
    open class func primaryKey() -> String? { "id" }
    open class func sharedSchema() -> ObjectSchema? {
        ObjectSchema(className: className(), primaryKeyProperty: primaryKey().map(Property.init))
    }
    public subscript(_ key: String) -> Any? {
        get { values[key] }
        set { values[key] = newValue }
    }
    public var rowID: String { values[type(of: self).primaryKey()!] as? String ?? "" }
    public func copyRow() -> Object { let copy = type(of: self).init(); copy.values = values; return copy }
}
public struct Results<Element: Object>: RandomAccessCollection {
    private let rows: [Element]
    public init(_ rows: [Element]) { self.rows = rows }
    public var startIndex: Int { rows.startIndex }
    public var endIndex: Int { rows.endIndex }
    public subscript(_ index: Int) -> Element { rows[index] }
    public func filter(_ query: String, _ names: [String]) -> Results<Element> {
        precondition(query == "entityType IN %@", "Unsupported collaborator query")
        return Results(rows.filter { names.contains($0["entityType"] as? String ?? "") })
    }
}
public enum RealmFixtureError: Error { case ownerHeld, frozenWrite }
public final class Realm: @unchecked Sendable {
    public enum UpdatePolicy { case modified }
    public private(set) var isInWriteTransaction = false
    public private(set) var isFrozen = false
    public private(set) var refreshCount = 0
    public private(set) var freezeCount = 0
    public var onRefresh: (() -> Void)?
    private var committed: [String: Object] = [:]
    private var provisional: [String: Object] = [:]
    private let types: [Object.Type]
    public var schema: Schema { Schema(objectSchema: types.compactMap { $0.sharedSchema() }) }
    public init(types: [Object.Type]) { self.types = types }
    private func key(_ type: Object.Type, _ id: String) -> String { type.className() + "|" + id }
    private func clones(_ values: [String: Object]) -> [String: Object] { values.mapValues { $0.copyRow() } }
    public func object<T: Object>(ofType type: T.Type, forPrimaryKey id: String) -> T? {
        let row = (isInWriteTransaction ? provisional : committed)[key(type,id)] as? T
        row?.realm = self
        return row
    }
    public func objects<T: Object>(_ type: T.Type) -> Results<T> {
        let rows = (isInWriteTransaction ? provisional : committed).values.compactMap { row -> T? in
            guard Swift.type(of: row).className() == type.className() else { return nil }
            return row as? T
        }
        rows.forEach { $0.realm = self }
        return Results(rows.sorted { $0.rowID < $1.rowID })
    }
    public func add(_ row: Object, update: UpdatePolicy = .modified) {
        precondition(isInWriteTransaction && !isFrozen)
        row.realm = self; provisional[key(type(of: row),row.rowID)] = row
    }
    public func delete(_ row: Object) {
        precondition(isInWriteTransaction && !isFrozen)
        provisional.removeValue(forKey: key(type(of: row),row.rowID))
    }
    public func beginWrite() throws {
        guard !isFrozen else { throw RealmFixtureError.frozenWrite }
        guard !isInWriteTransaction else { throw RealmFixtureError.ownerHeld }
        provisional = clones(committed); isInWriteTransaction = true
    }
    public func commitWrite() {
        precondition(isInWriteTransaction)
        committed = clones(provisional); provisional = [:]; isInWriteTransaction = false
    }
    public func cancelWrite() {
        precondition(isInWriteTransaction)
        provisional = [:]; isInWriteTransaction = false
    }
    public func write(_ operation: () throws -> Void) throws {
        try beginWrite()
        do { try operation(); commitWrite() } catch { cancelWrite(); throw error }
    }
    @discardableResult public func refresh() -> Bool {
        precondition(!isFrozen)
        refreshCount += 1
        let callback = onRefresh; onRefresh = nil; callback?()
        return !isInWriteTransaction
    }
    public func freeze() -> Realm {
        freezeCount += 1
        if isFrozen { return self }
        let result = Realm(types: types); result.committed = clones(committed); result.isFrozen = true
        return result
    }
}
public enum ObjectiveCSupport { public static func convert(object: Realm) -> Realm { object } }
