//
//  KeyValueStore.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation
import Darwin

/// Creates every missing path component and durably publishes each directory
/// entry in its parent before returning. A leaf-directory fsync alone cannot
/// prove that a newly-created namespace survives power loss: the entry that
/// names that directory belongs to its parent.
internal func bigSyncCreateDirectoryDurably(
    at directoryURL: URL,
    fileManager: FileManager = .default
) throws {
    let targetURL = directoryURL.standardizedFileURL
    var missingDirectories = [URL]()
    var cursor = targetURL
    var isDirectory: ObjCBool = false

    while !fileManager.fileExists(
        atPath: cursor.path,
        isDirectory: &isDirectory
    ) {
        missingDirectories.append(cursor)
        let parent = cursor.deletingLastPathComponent().standardizedFileURL
        guard parent.path != cursor.path else {
            throw POSIXError(.ENOENT)
        }
        cursor = parent
    }
    guard isDirectory.boolValue else {
        throw CocoaError(.fileWriteFileExists)
    }

    for directory in missingDirectories.reversed() {
        do {
            try fileManager.createDirectory(
                at: directory,
                withIntermediateDirectories: false
            )
        } catch {
            var racedDirectory: ObjCBool = false
            guard fileManager.fileExists(
                atPath: directory.path,
                isDirectory: &racedDirectory
            ), racedDirectory.boolValue else {
                throw error
            }
        }
        try bigSyncSynchronizeDirectory(
            at: directory.deletingLastPathComponent()
        )
    }
}

internal func bigSyncSynchronizeDirectory(at directoryURL: URL) throws {
    let descriptor = Darwin.open(directoryURL.path, O_RDONLY | O_DIRECTORY)
    guard descriptor >= 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    defer { Darwin.close(descriptor) }
    guard Darwin.fsync(descriptor) == 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}


/// Interface for persisting and loading values.
@objc public protocol KeyValueStore {
    
    
    /// Returns the object associated with the specified key.
    /// - Parameter defaultName: A key in the current store.
    func object(forKey defaultName: String) -> Any?
    
    /// Returns the Boolean value associated with the specified key.
    /// - Parameter defaultName: A key in the current store.
    func bool(forKey defaultName: String) -> Bool
    
    /// Sets the value of the specified key.
    /// - Parameters:
    ///   - value: The object to store in the store.
    ///   - defaultName: The key with which to associate the value.
    func set(value: Any?, forKey defaultName: String)
    
    /// Sets the value of the specified key to the specified Boolean value.
    /// - Parameters:
    ///   - boolValue: The Boolean value to store.
    ///   - defaultName: The key with which to associate the value.
    func set(boolValue: Bool, forKey defaultName: String)
    
    /// Removes the value of the specified default key.
    /// - Parameter defaultName: The key whose value you want to remove.
    func removeObject(forKey defaultName: String)

    /// Flushes pending mutations to durable storage.
    ///
    /// CloudKit lifecycle/reset envelopes use this as a crash-consistency
    /// boundary before clearing cursors or tracking state. Custom stores that
    /// don't implement it fail closed for those operations rather than
    /// claiming that an in-memory read-back is durable.
    @objc optional func synchronize() -> Bool
}

/// A `KeyValueStore` whose reads and mutations have an explicit durable error
/// boundary. BigSyncKit's production store implements this protocol so a
/// corrupt plist or failed atomic replacement can never be mistaken for an
/// empty namespace or a committed cursor.
public protocol DurableKeyValueStore: KeyValueStore {
    /// Opens and durably rewrites the current property-list snapshot. This is
    /// used during production construction to prove that the namespace is
    /// readable and writable before a Realm provider or synchronizer exists.
    func prepareForUse() throws

    /// Reloads the current snapshot and throws if storage is unreadable or if
    /// a prior mutation has not been durably committed.
    func validateDurability() throws

    func durableObject(forKey defaultName: String) throws -> Any?
    func setDurably(value: Any?, forKey defaultName: String) throws
    func removeDurably(forKey defaultName: String) throws
}

public enum DurableKeyValueStoreError: Error, LocalizedError {
    case unavailable
    case mutationNotDurable

    public var errorDescription: String? {
        switch self {
        case .unavailable:
            return "The durable key-value store is unavailable."
        case .mutationNotDurable:
            return "The durable key-value mutation could not be committed."
        }
    }
}

extension KeyValueStore {
    internal func bigSyncValidateDurability() throws {
        if let durableStore = self as? any DurableKeyValueStore {
            try durableStore.validateDurability()
            return
        }
        guard synchronize?() == true else {
            throw DurableKeyValueStoreError.unavailable
        }
    }

    internal func bigSyncDurableObject(forKey key: String) throws -> Any? {
        if let durableStore = self as? any DurableKeyValueStore {
            return try durableStore.durableObject(forKey: key)
        }
        try bigSyncValidateDurability()
        return object(forKey: key)
    }

    internal func bigSyncSetDurably(value: Any?, forKey key: String) throws {
        if let durableStore = self as? any DurableKeyValueStore {
            try durableStore.setDurably(value: value, forKey: key)
            return
        }
        set(value: value, forKey: key)
        try bigSyncValidateDurability()
        guard Self.bigSyncPropertyListsEqual(object(forKey: key), value) else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
    }

    internal func bigSyncRemoveDurably(forKey key: String) throws {
        if let durableStore = self as? any DurableKeyValueStore {
            try durableStore.removeDurably(forKey: key)
            return
        }
        removeObject(forKey: key)
        try bigSyncValidateDurability()
        guard object(forKey: key) == nil else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
    }

    private static func bigSyncPropertyListsEqual(_ lhs: Any?, _ rhs: Any?) -> Bool {
        switch (lhs, rhs) {
        case (nil, nil):
            return true
        case let (lhs?, rhs?):
            return (lhs as? NSObject)?.isEqual(rhs) == true
        default:
            return false
        }
    }
}


/// Implementation of `KeyValueStore` using `UserDefaults`
@objc public class UserDefaultsAdapter: NSObject, KeyValueStore {
    
    
    /// `UserDefaults` used internally by this adapter.
    @objc public let userDefaults: UserDefaults
    
    /// Creates a new `UserDefaultsAdapter` with the given default.
    /// - Parameter userDefaults: `UserDefaults` instance.
    @objc public init(userDefaults: UserDefaults) {
        self.userDefaults = userDefaults
    }
    
    /// Returns the object associated with the specified key.
    /// - Parameter defaultName: A key in the current store.
    @objc public func object(forKey defaultName: String) -> Any? {
        return userDefaults.object(forKey: defaultName)
    }
    
    /// Returns the Boolean value associated with the specified key.
    /// - Parameter defaultName: A key in the current store.
    @objc public func bool(forKey defaultName: String) -> Bool {
        return userDefaults.bool(forKey: defaultName)
    }
    
    /// Sets the value of the specified key.
    /// - Parameters:
    ///   - value: The object to store in the store.
    ///   - defaultName: The key with which to associate the value.
    @objc public func set(value: Any?, forKey defaultName: String) {
        userDefaults.set(value, forKey: defaultName)
    }
    
    /// Sets the value of the specified key to the specified Boolean value.
    /// - Parameters:
    ///   - boolValue: The Boolean value to store.
    ///   - defaultName: The key with which to associate the value.
    @objc public func set(boolValue: Bool, forKey defaultName: String) {
        userDefaults.set(boolValue, forKey: defaultName)
    }
    
    /// Removes the value of the specified default key.
    /// - Parameter defaultName: The key whose value you want to remove.
    @objc public func removeObject(forKey defaultName: String) {
        userDefaults.removeObject(forKey: defaultName)
    }

    @objc public func synchronize() -> Bool {
        userDefaults.synchronize()
    }
}

/// A synchronous, file-backed `KeyValueStore` for an isolated synchronizer
/// client. Values are kept in a property-list file at the caller-supplied URL;
/// every mutation replaces that file atomically by default.
///
/// This store is thread-safe for callers sharing an instance. It is intended
/// for one client namespace; separate instances must use separate file URLs.
@objc public final class FileKeyValueStore: NSObject, KeyValueStore, DurableKeyValueStore {
    public let fileURL: URL
    public let writesAtomically: Bool

    private let lock = NSLock()
    /// The most recent durable-state error.  The `KeyValueStore` Objective-C
    /// surface predates throwing mutations, so callers that need an immediate
    /// error can inspect this in addition to the false returned by
    /// `synchronize()`.  We intentionally retain an unreadable-file error:
    /// treating corrupt existing state as an empty dictionary could cause a
    /// synchronizer to create fresh cursors over an old namespace.
    @objc public private(set) var lastPersistenceError: NSError?
    private var hasUncommittedMutationFailure = false

    private let lockFileURL: URL
    private let beforeAtomicReplace: (() throws -> Void)?

    private final class UnreadableValue: NSObject {
        static let shared = UnreadableValue()
    }

    private enum LoadedStorage {
        case missing
        case value([String: Any])
        case unreadable(NSError)
    }

    @objc public init(fileURL: URL, writesAtomically: Bool = true) {
        self.fileURL = fileURL.standardizedFileURL
        self.writesAtomically = writesAtomically
        beforeAtomicReplace = nil
        lockFileURL = fileURL.standardizedFileURL
            .appendingPathExtension("lock")
        super.init()
        lock.withLock {
            _ = reloadStorage(lockMode: LOCK_SH)
        }
    }

    /// Fault-injection seam for behavioral tests of the atomic commit
    /// boundary. Production always uses the public initializer above.
    internal init(
        fileURL: URL,
        writesAtomically: Bool = true,
        beforeAtomicReplace: @escaping () throws -> Void
    ) {
        self.fileURL = fileURL.standardizedFileURL
        self.writesAtomically = writesAtomically
        self.beforeAtomicReplace = beforeAtomicReplace
        lockFileURL = fileURL.standardizedFileURL
            .appendingPathExtension("lock")
        super.init()
        lock.withLock {
            _ = reloadStorage(lockMode: LOCK_SH)
        }
    }

    @objc public func object(forKey defaultName: String) -> Any? {
        lock.withLock {
            guard let storage = reloadStorage(lockMode: LOCK_SH) else {
                // Deliberately non-nil. A malformed, nonempty plist must not
                // be indistinguishable from a newly-created empty store.
                return UnreadableValue.shared
            }
            return storage[defaultName]
        }
    }

    @objc public func bool(forKey defaultName: String) -> Bool {
        lock.withLock {
            guard let storage = reloadStorage(lockMode: LOCK_SH) else {
                return false
            }
            return storage[defaultName] as? Bool ?? false
        }
    }

    @objc public func set(value: Any?, forKey defaultName: String) {
        lock.withLock {
            let acquiredLock = withFileLock(mode: LOCK_EX) {
                guard var updatedStorage = reloadStorageLocked() else { return }
                guard !hasUncommittedMutationFailure else { return }
                if let value {
                    updatedStorage[defaultName] = value
                } else {
                    updatedStorage.removeValue(forKey: defaultName)
                }
                persist(updatedStorage)
            }
            if !acquiredLock {
                hasUncommittedMutationFailure = true
            }
        }
    }

    @objc public func set(boolValue: Bool, forKey defaultName: String) {
        set(value: boolValue, forKey: defaultName)
    }

    @objc public func removeObject(forKey defaultName: String) {
        lock.withLock {
            let acquiredLock = withFileLock(mode: LOCK_EX) {
                guard var updatedStorage = reloadStorageLocked() else { return }
                guard !hasUncommittedMutationFailure else { return }
                updatedStorage.removeValue(forKey: defaultName)
                persist(updatedStorage)
            }
            if !acquiredLock {
                hasUncommittedMutationFailure = true
            }
        }
    }

    @objc public func synchronize() -> Bool {
        lock.withLock {
            guard reloadStorage(lockMode: LOCK_SH) != nil else { return false }
            return lastPersistenceError == nil
        }
    }

    public func prepareForUse() throws {
        try lock.withLock {
            try withFileLockThrowing(mode: LOCK_EX) {
                let storage = try reloadStorageLockedThrowing()
                if hasUncommittedMutationFailure {
                    throw lastPersistenceError
                        ?? DurableKeyValueStoreError.mutationNotDurable
                }
                try persistThrowing(storage)
            }
        }
    }

    public func validateDurability() throws {
        try lock.withLock {
            try withFileLockThrowing(mode: LOCK_SH) {
                _ = try reloadStorageLockedThrowing()
                if hasUncommittedMutationFailure {
                    throw lastPersistenceError
                        ?? DurableKeyValueStoreError.mutationNotDurable
                }
            }
        }
    }

    public func durableObject(forKey defaultName: String) throws -> Any? {
        try lock.withLock {
            try withFileLockThrowing(mode: LOCK_SH) {
                let storage = try reloadStorageLockedThrowing()
                if hasUncommittedMutationFailure {
                    throw lastPersistenceError
                        ?? DurableKeyValueStoreError.mutationNotDurable
                }
                return storage[defaultName]
            }
        }
    }

    public func setDurably(value: Any?, forKey defaultName: String) throws {
        try lock.withLock {
            do {
                try withFileLockThrowing(mode: LOCK_EX) {
                    var storage = try reloadStorageLockedThrowing()
                    if hasUncommittedMutationFailure {
                        throw lastPersistenceError
                            ?? DurableKeyValueStoreError.mutationNotDurable
                    }
                    if let value {
                        storage[defaultName] = value
                    } else {
                        storage.removeValue(forKey: defaultName)
                    }
                    try persistThrowing(storage)
                }
            } catch {
                // A throwing caller observes this failure immediately, while
                // the retained mutation error also prevents a later terminal
                // sync boundary from silently succeeding after a plain read.
                hasUncommittedMutationFailure = true
                lastPersistenceError = error as NSError
                throw error
            }
        }
    }

    public func removeDurably(forKey defaultName: String) throws {
        try setDurably(value: nil, forKey: defaultName)
    }

    /// Acquires an advisory lock shared by all `FileKeyValueStore` instances
    /// targeting this namespace. Mutations reload only after gaining an
    /// exclusive lock, eliminating stale read/modify/write lost updates.
    private func reloadStorage(lockMode: Int32) -> [String: Any]? {
        var storage: [String: Any]?
        guard withFileLock(mode: lockMode, {
            storage = reloadStorageLocked()
        }) else { return nil }
        return storage
    }

    @discardableResult
    private func withFileLock(mode: Int32, _ body: () -> Void) -> Bool {
        do {
            try withFileLockThrowing(mode: mode, body)
            return true
        } catch {
            lastPersistenceError = error as NSError
            return false
        }
    }

    private func withFileLockThrowing<T>(
        mode: Int32,
        _ body: () throws -> T
    ) throws -> T {
        try bigSyncCreateDirectoryDurably(
            at: fileURL.deletingLastPathComponent()
        )
        let descriptor = Darwin.open(
            lockFileURL.path,
            O_CREAT | O_RDWR,
            S_IRUSR | S_IWUSR
        )
        guard descriptor >= 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        defer { Darwin.close(descriptor) }
        try markExcludedFromBackup(lockFileURL)
        guard flock(descriptor, mode) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        defer { _ = flock(descriptor, LOCK_UN) }
        return try body()
    }

    private func reloadStorageLocked() -> [String: Any]? {
        switch Self.loadStorage(from: fileURL) {
            case .missing:
                if !hasUncommittedMutationFailure { lastPersistenceError = nil }
                return [:]
            case let .value(storage):
                if !hasUncommittedMutationFailure { lastPersistenceError = nil }
                return storage
            case let .unreadable(error):
                lastPersistenceError = error
                return nil
        }
    }

    private func reloadStorageLockedThrowing() throws -> [String: Any] {
        switch Self.loadStorage(from: fileURL) {
        case .missing:
            if !hasUncommittedMutationFailure { lastPersistenceError = nil }
            return [:]
        case let .value(storage):
            if !hasUncommittedMutationFailure { lastPersistenceError = nil }
            return storage
        case let .unreadable(error):
            lastPersistenceError = error
            throw error
        }
    }

    private func persist(_ updatedStorage: [String: Any]) {
        do {
            try persistThrowing(updatedStorage)
        } catch {
            hasUncommittedMutationFailure = true
            lastPersistenceError = error as NSError
        }
    }

    private func persistThrowing(_ updatedStorage: [String: Any]) throws {
        guard PropertyListSerialization.propertyList(updatedStorage, isValidFor: .binary) else {
            hasUncommittedMutationFailure = true
            let error = NSError(
                domain: "FileKeyValueStore",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "FileKeyValueStore accepts property-list values only"]
            )
            lastPersistenceError = error
            throw error
        }
        do {
            let data = try PropertyListSerialization.data(
                fromPropertyList: updatedStorage,
                format: .binary,
                options: 0
            )
            if writesAtomically {
                let temporaryURL = fileURL.deletingLastPathComponent()
                    .appendingPathComponent(".\(fileURL.lastPathComponent).\(UUID().uuidString).tmp")
                var shouldRemoveTemporaryFile = true
                defer {
                    if shouldRemoveTemporaryFile,
                       FileManager.default.fileExists(atPath: temporaryURL.path) {
                        try? FileManager.default.removeItem(at: temporaryURL)
                    }
                }
                try data.write(to: temporaryURL)
                try markExcludedFromBackup(temporaryURL)
                let handle = try FileHandle(forWritingTo: temporaryURL)
                try handle.synchronize()
                try handle.close()
                try beforeAtomicReplace?()
                guard Darwin.rename(temporaryURL.path, fileURL.path) == 0 else {
                    throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
                }
                shouldRemoveTemporaryFile = false
            } else {
                try data.write(to: fileURL)
                let handle = try FileHandle(forWritingTo: fileURL)
                try handle.synchronize()
                try handle.close()
            }
            try synchronizeDirectory()
            guard case let .value(persisted) = Self.loadStorage(from: fileURL),
                  NSDictionary(dictionary: persisted).isEqual(to: updatedStorage) else {
                throw NSError(
                    domain: "FileKeyValueStore",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: "FileKeyValueStore could not verify its persisted plist"]
                )
            }
            hasUncommittedMutationFailure = false
            lastPersistenceError = nil
        } catch {
            hasUncommittedMutationFailure = true
            lastPersistenceError = error as NSError
            throw error
        }
    }

    private func markExcludedFromBackup(_ url: URL) throws {
        var mutableURL = url
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        try mutableURL.setResourceValues(values)
    }

    private func synchronizeDirectory() throws {
        try bigSyncSynchronizeDirectory(
            at: fileURL.deletingLastPathComponent()
        )
    }

    private static func loadStorage(from fileURL: URL) -> LoadedStorage {
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return .missing }
        do {
            let data = try Data(contentsOf: fileURL)
            let propertyList = try PropertyListSerialization.propertyList(
                from: data,
                options: [],
                format: nil
            )
            guard let storage = propertyList as? [String: Any] else {
                throw NSError(
                    domain: "FileKeyValueStore",
                    code: 3,
                    userInfo: [NSLocalizedDescriptionKey: "FileKeyValueStore plist root is not a dictionary"]
                )
            }
            return .value(storage)
        } catch {
            return .unreadable(error as NSError)
        }
    }
}
