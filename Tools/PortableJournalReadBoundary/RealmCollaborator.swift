// Explicit in-memory committed/provisional views, not the Realm SDK.
import Foundation

public enum TestValue: Equatable, Sendable {
    case string(String), date(Date), bool(Bool)
}
open class Object: @unchecked Sendable {
    public var fields: [String: TestValue] = [:]
    public required init() {}
    open class func className() -> String { String(describing: self) }
    public var key: String { string("key") ?? "" }
    public func string(_ name: String) -> String? {
        if case let .string(value)? = fields[name] { return value }; return nil
    }
    public func set(_ name: String, _ value: String?) { fields[name] = value.map(TestValue.string) }
    public func date(_ name: String) -> Date {
        if case let .date(value)? = fields[name] { return value }; return .distantPast
    }
    public func bool(_ name: String) -> Bool {
        if case let .bool(value)? = fields[name] { return value }; return false
    }
    public func clone() -> Self { let value = type(of: self).init(); value.fields = fields; return value }
}
public struct ObjectSchema: Sendable { public let className: String }
public struct Schema: Sendable { public let objectSchema: [ObjectSchema] }
public struct Results<T: Object>: RandomAccessCollection {
    private let realm: Realm
    private let select: ([T]) -> [T]
    public init(_ realm: Realm, select: @escaping ([T]) -> [T] = { $0 }) {
        self.realm = realm; self.select = select
    }
    private var rows: [T] { select(realm.all(T.self)) }
    public var startIndex: Int { 0 }
    public var endIndex: Int { rows.count }
    public subscript(index: Int) -> T { rows[index] }
    public func filter(_ format: String, _ names: [String]) -> Results<T> {
        precondition(format == "entityType IN %@")
        return Results(realm) { input in self.select(input).filter { names.contains($0.string("entityType") ?? "") } }
    }
    public func sorted(byKeyPath field: String) -> Results<T> {
        Results(realm) { input in self.select(input).sorted { ($0.string(field) ?? "") < ($1.string(field) ?? "") } }
    }
    public func freeze() -> Results<T> { Results(realm.freeze(), select: select) }
}
public final class Realm: @unchecked Sendable {
    public let schema: Schema
    private var live: [String: Object] = [:]
    private var committed: [String: Object] = [:]
    public private(set) var isInWriteTransaction = false
    public private(set) var isFrozen = false
    public private(set) var refreshCount = 0
    public var onRefresh: (() -> Void)?
    public init(types: [String]) { schema = Schema(objectSchema: types.map { ObjectSchema(className: $0) }) }
    private static func name(_ type: Object.Type, _ key: String) -> String { type.className() + ":" + key }
    public func object<T: Object>(ofType type: T.Type, forPrimaryKey key: String) -> T? {
        live[Self.name(type, key)] as? T
    }
    public func objects<T: Object>(_ type: T.Type) -> Results<T> { Results(self) }
    public func all<T: Object>(_ type: T.Type) -> [T] { live.values.compactMap { $0 as? T } }
    public func add(_ value: Object) {
        precondition(isInWriteTransaction); live[Self.name(type(of: value), value.key)] = value
    }
    public func delete(_ value: Object) {
        precondition(isInWriteTransaction); live.removeValue(forKey: Self.name(type(of: value), value.key))
    }
    public func beginWrite() { precondition(!isFrozen && !isInWriteTransaction); isInWriteTransaction = true }
    public func commitWrite() { precondition(isInWriteTransaction); committed = live.mapValues { $0.clone() }; isInWriteTransaction = false }
    public func cancelWrite() { precondition(isInWriteTransaction); live = committed.mapValues { $0.clone() }; isInWriteTransaction = false }
    public func write(_ body: () throws -> Void) rethrows {
        beginWrite(); do { try body(); commitWrite() } catch { cancelWrite(); throw error }
    }
    @discardableResult public func refresh() -> Bool { refreshCount += 1; onRefresh?(); return true }
    public func freeze() -> Realm {
        if isFrozen { return self }
        let value = Realm(types: schema.objectSchema.map(\.className))
        value.live = committed.mapValues { $0.clone() }
        value.committed = value.live; value.isFrozen = true
        return value
    }
}
