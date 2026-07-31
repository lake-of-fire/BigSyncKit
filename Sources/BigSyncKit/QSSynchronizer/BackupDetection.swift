//
//  QSBackupDetection.swift
//  Pods
//
//  Created by Manuel Entrena on 04/04/2019.
//

import Foundation

enum BackupDetection {
    enum DetectionResult: Int {
        case firstRun
        case restoredFromBackup
        case regularLaunch
    }

    static let storeKey = "QSBackupDetectionStoreKey.v2"

    private static var applicationSupportDirectory: URL {
        #if os(iOS) || os(watchOS)
        FileManager.default.urls(for: .libraryDirectory, in: .userDomainMask)[0]
        #else
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("com.mentrena.QSCloudKitSynchronizer", isDirectory: true)
        #endif
    }

    static var defaultSentinelURL: URL {
        applicationSupportDirectory
            .appendingPathComponent("backupDetection.v2", isDirectory: false)
    }

    /// Detects a restored installation by pairing a defaults marker, which is
    /// expected to be restored from backup, with a filesystem sentinel that is
    /// explicitly excluded from backup.
    ///
    /// The marker is written only after the sentinel has been created and marked
    /// as excluded. A failed sentinel write therefore cannot make the next launch
    /// look like a restore.
    static func run(
        store: KeyValueStore,
        fileManager: FileManager = .default,
        sentinelURL: URL = defaultSentinelURL
    ) throws -> DetectionResult {
        let sentinelExists = fileManager.fileExists(atPath: sentinelURL.path)
        let markerExists = store.bool(forKey: storeKey)

        let result: DetectionResult
        if sentinelExists {
            result = .regularLaunch
        } else if markerExists {
            result = .restoredFromBackup
        } else {
            result = .firstRun
        }

        if !sentinelExists {
            try fileManager.createDirectory(
                at: sentinelURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try Data("Backup detection file\n".utf8).write(
                to: sentinelURL,
                options: .atomic
            )
            var resourceValues = URLResourceValues()
            resourceValues.isExcludedFromBackup = true
            var mutableSentinelURL = sentinelURL
            try mutableSentinelURL.setResourceValues(resourceValues)
        }

        // Repair older or partially initialized installations that have the
        // excluded sentinel but never persisted the backed-up marker.
        if !markerExists {
            store.set(boolValue: true, forKey: storeKey)
        }

        return result
    }
}
