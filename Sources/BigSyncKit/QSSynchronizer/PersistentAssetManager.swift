//
//  PersistentAssetManager.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import CryptoKit
import Darwin
import Foundation

class PersistentAssetManager {
    let identifier: String
    let rootDirectoryURL: URL?

    init(identifier: String, rootDirectoryURL: URL? = nil) {
        self.identifier = identifier
        self.rootDirectoryURL = rootDirectoryURL
    }

    private struct AssetKey: Hashable {
        let recordID: String
        let propertyName: String
        let digest: String
    }

    private var cachedAssets: [AssetKey: URL] = [:]
    private let cacheQueue = DispatchQueue(label: "PersistentAssetManager.Cache")
    
    private lazy var assetDirectory: URL = {
        let defaultRootURL = FileManager.default.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        )[0].appendingPathComponent("CloudKitAssets")
        let directoryURL = (rootDirectoryURL ?? defaultRootURL)
            .appendingPathComponent(identifier)
        
        if !FileManager.default.fileExists(atPath: directoryURL.path) {
            try? FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true, attributes: nil)
        }
        
        return directoryURL
    }()
    
    func store(data: Data, forRecordID recordID: String, propertyName: String) throws -> URL {
        let digest = Self.digestString(for: data)
        let cacheKey = AssetKey(recordID: recordID, propertyName: propertyName, digest: digest)
        if let cachedURL = cacheQueue.sync(execute: { cachedAssets[cacheKey] }) {
            if FileManager.default.fileExists(atPath: cachedURL.path) {
                return cachedURL
            }
            _ = cacheQueue.sync {
                cachedAssets.removeValue(forKey: cacheKey)
            }
        }

        let unique = ProcessInfo.processInfo.globallyUniqueString
        let fileName = "\(Self.fileNamePrefix(forRecordID: recordID))_\(unique)"
        let url = assetDirectory.appendingPathComponent(fileName)
        try data.write(to: url, options: .atomicWrite)
        cacheQueue.sync {
            cachedAssets[cacheKey] = url
        }
//        debugPrint("# wrote:", url.lastPathComponent)
        return url
    }

    private static func digestString(for data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    static func fileNamePrefix(forRecordID recordID: String) -> String {
        "record-" + digestString(for: Data(recordID.utf8))
    }
    
    /// Removes materialized CKAsset files after the synchronization operation
    /// that owned them has reached a terminal import boundary. Pending Realm
    /// data remains the source of truth and will rematerialize an asset for the
    /// next prepared generation. Keeping every historical file for a pending
    /// record causes unbounded growth during repeated offline edits.
    func clearAssetFiles() {
        let directoryURL = assetDirectory
        // Foundation directory enumeration can route through CoreServices and
        // block while resolving unrelated mounted volumes. These directories
        // contain only flat files owned by this manager, so POSIX enumeration
        // avoids making a sync/reset dependent on volume metadata discovery.
        guard let directory = opendir(directoryURL.path) else {
            return
        }
        defer { closedir(directory) }

        while let entry = readdir(directory) {
            let fileName = withUnsafePointer(to: &entry.pointee.d_name) {
                $0.withMemoryRebound(to: CChar.self, capacity: 1) {
                    String(cString: $0)
                }
            }
            guard fileName != ".", fileName != ".." else { continue }
            let filePath = directoryURL
                .appendingPathComponent(fileName)
                .path
            _ = unlink(filePath)
        }

        cacheQueue.sync {
            cachedAssets.removeAll(keepingCapacity: false)
        }
    }
}
