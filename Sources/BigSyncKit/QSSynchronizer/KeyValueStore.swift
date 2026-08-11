//
//  KeyValueStore.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation


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
}

/// A synchronous, file-backed `KeyValueStore` for an isolated synchronizer
/// client. Values are kept in a property-list file at the caller-supplied URL;
/// every mutation replaces that file atomically by default.
///
/// This store is thread-safe for callers sharing an instance. It is intended
/// for one client namespace; separate instances must use separate file URLs.
@objc public final class FileKeyValueStore: NSObject, KeyValueStore {
    public let fileURL: URL
    public let writesAtomically: Bool

    private let lock = NSLock()
    private var storage: [String: Any]

    @objc public init(fileURL: URL, writesAtomically: Bool = true) {
        self.fileURL = fileURL.standardizedFileURL
        self.writesAtomically = writesAtomically
        storage = Self.loadStorage(from: fileURL.standardizedFileURL)
        super.init()
    }

    @objc public func object(forKey defaultName: String) -> Any? {
        lock.withLock {
            storage[defaultName]
        }
    }

    @objc public func bool(forKey defaultName: String) -> Bool {
        lock.withLock {
            storage[defaultName] as? Bool ?? false
        }
    }

    @objc public func set(value: Any?, forKey defaultName: String) {
        lock.withLock {
            var updatedStorage = storage
            if let value {
                updatedStorage[defaultName] = value
            } else {
                updatedStorage.removeValue(forKey: defaultName)
            }
            persist(updatedStorage)
        }
    }

    @objc public func set(boolValue: Bool, forKey defaultName: String) {
        set(value: boolValue, forKey: defaultName)
    }

    @objc public func removeObject(forKey defaultName: String) {
        lock.withLock {
            var updatedStorage = storage
            updatedStorage.removeValue(forKey: defaultName)
            persist(updatedStorage)
        }
    }

    private static func loadStorage(from fileURL: URL) -> [String: Any] {
        guard let data = try? Data(contentsOf: fileURL),
              let propertyList = try? PropertyListSerialization.propertyList(
                from: data,
                options: [],
                format: nil
              ),
              let storage = propertyList as? [String: Any] else {
            return [:]
        }
        return storage
    }

    private func persist(_ updatedStorage: [String: Any]) {
        guard PropertyListSerialization.propertyList(updatedStorage, isValidFor: .binary) else {
            assertionFailure("FileKeyValueStore accepts property-list values only")
            return
        }
        do {
            try FileManager.default.createDirectory(
                at: fileURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            let data = try PropertyListSerialization.data(
                fromPropertyList: updatedStorage,
                format: .binary,
                options: 0
            )
            try data.write(
                to: fileURL,
                options: writesAtomically ? .atomic : []
            )
            storage = updatedStorage
        } catch {
            assertionFailure("Unable to persist FileKeyValueStore: \(error)")
        }
    }
}
