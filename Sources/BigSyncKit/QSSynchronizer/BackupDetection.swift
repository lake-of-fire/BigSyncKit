//
//  QSBackupDetection.swift
//  Pods
//
//  Created by Manuel Entrena on 04/04/2019.
//

import Foundation
import Darwin

/// Detects a restored installation without sharing reset state between
/// independently configured synchronizers.
///
/// `namespace` must be the caller's complete durable synchronizer identity
/// (container, database scope, and zone).  v4 deliberately does not import
/// the old global v2/v3 keys: those keys cannot establish which CloudKit
/// client owns their state.  This is a clean cut for sync *tracking* only;
/// a new client performs its normal CloudKit reconciliation rather than
/// treating another client's marker as authoritative.
enum BackupDetection {
    private static let markerData = Data("BigSyncKit backup marker v4\n".utf8)

    enum DetectionResult: Int {
        case firstRun
        case restoredFromBackup
        case regularLaunch
    }

    enum Error: Swift.Error, Equatable {
        /// The restored-backup event must survive before the sentinel is
        /// recreated. Otherwise a crash can permanently hide recovery work.
        case restoreEventPersistenceVerificationFailed
        /// The backup-eligible marker must be durable before it can identify
        /// a future restoration. A failed write is removed and retried on a
        /// later regular launch.
        case markerPersistenceVerificationFailed
        /// Recovery completed, but removing its durable event could not be
        /// committed to the containing directory.
        case restoreEventAcknowledgementVerificationFailed
    }

    private static var applicationSupportDirectory: URL {
        #if os(iOS) || os(watchOS)
        FileManager.default.urls(for: .libraryDirectory, in: .userDomainMask)[0]
        #else
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("com.mentrena.QSCloudKitSynchronizer", isDirectory: true)
        #endif
    }

    private static func synchronizeFile(at url: URL) throws {
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        try handle.synchronize()
    }

    private static func synchronizeParentDirectory(of url: URL) throws {
        let directoryURL = url.deletingLastPathComponent()
        let descriptor = Darwin.open(directoryURL.path, O_RDONLY)
        guard descriptor >= 0 else {
            throw NSError(
                domain: NSPOSIXErrorDomain,
                code: Int(errno)
            )
        }
        defer { Darwin.close(descriptor) }
        guard Darwin.fsync(descriptor) == 0 else {
            throw NSError(
                domain: NSPOSIXErrorDomain,
                code: Int(errno)
            )
        }
    }

    private static func synchronizePublishedFile(at url: URL) throws {
        try synchronizeFile(at: url)
        try synchronizeParentDirectory(of: url)
    }

    /// An encoded, reversible component avoids collisions from separators in
    /// user-provided CloudKit identifiers while remaining safe in defaults keys
    /// and filenames.
    private static func namespaceComponent(_ namespace: String) -> String {
        Data(namespace.utf8)
            .base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    /// Produces a namespace-specific sentinel under an optional location
    /// supplied by the host app. Passing the same `sharedBaseURL` lets an app
    /// and extension intentionally observe the same installation sentinel.
    static func defaultSentinelURL(
        namespace: String,
        sharedBaseURL: URL? = nil
    ) -> URL {
        (sharedBaseURL ?? applicationSupportDirectory)
            .appendingPathComponent(
                "backupDetection.v4." + namespaceComponent(namespace),
                isDirectory: false
            )
    }

    /// The restore event intentionally lives beside the sentinel rather than
    /// in `KeyValueStore`: a defaults read-back can be satisfied from memory
    /// even when its eventual disk write fails. This ordinary (not
    /// backup-excluded) file is atomically written and verified before the
    /// restore sentinel is recreated.
    static func restoreEventURL(sentinelURL: URL) -> URL {
        sentinelURL.appendingPathExtension("restore-event")
    }

    /// This ordinary file intentionally remains eligible for backup. It is
    /// paired with the backup-excluded sentinel to distinguish a restored
    /// installation from a first launch. Keeping the pair in the same
    /// namespace makes an app group safe for its app and extensions while
    /// preventing unrelated synchronizers from sharing recovery state.
    static func markerURL(sentinelURL: URL) -> URL {
        sentinelURL.appendingPathExtension("marker")
    }

    static func restoreEventURL(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil
    ) -> URL {
        restoreEventURL(sentinelURL: defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        ))
    }

    /// Detects a restored installation by pairing a backed-up per-client
    /// marker file with a per-client filesystem sentinel excluded from backup.
    ///
    /// For the restore path, the durable event is written and read back before
    /// any sentinel mutation. A store that cannot prove that write therefore
    /// fails closed with no new sentinel, so the next launch can retry safely.
    static func run(
        store: KeyValueStore,
        namespace: String,
        fileManager: FileManager = .default,
        sentinelURL: URL,
        eventWriter: ((URL, Data) throws -> Void)? = nil,
        markerWriter: ((URL, Data) throws -> Void)? = nil
    ) throws -> DetectionResult {
        precondition(!namespace.isEmpty, "BackupDetection requires a durable client namespace")

        let sentinelExists = fileManager.fileExists(atPath: sentinelURL.path)
        let uncachedSentinelURL = URL(fileURLWithPath: sentinelURL.path)
        let sentinelIsExcluded = sentinelExists
            && (try? uncachedSentinelURL.resourceValues(
                forKeys: [.isExcludedFromBackupKey]
            ).isExcludedFromBackup) == true
        let markerURL = markerURL(sentinelURL: sentinelURL)
        let markerExists = persistedMarkerExists(at: markerURL)

        let result: DetectionResult
        if sentinelIsExcluded {
            result = .regularLaunch
        } else if markerExists {
            result = .restoredFromBackup
        } else {
            result = .firstRun
        }

        if result == .restoredFromBackup {
            try persistRestoreEvent(
                at: restoreEventURL(sentinelURL: sentinelURL),
                fileManager: fileManager,
                eventWriter: eventWriter
            )
        }

        if !sentinelIsExcluded {
            // A legacy/crash-prefix sentinel that lacks the exclusion bit is
            // not valid installation proof. If its backed-up marker exists it
            // is conservatively a restore; otherwise it is a first run. Replace
            // only this BigSyncKit tracking file after any required restore
            // event has been made durable.
            if sentinelExists {
                try fileManager.removeItem(at: sentinelURL)
            }
            try createExcludedSentinel(
                at: sentinelURL,
                fileManager: fileManager
            )
        }

        // `store` is deliberately retained in this internal API for source
        // compatibility with existing callers. v4 no longer records backup
        // detection state in defaults: a defaults read-back can be satisfied
        // from memory while its durable write fails.
        _ = store

        // Write the backed-up marker only after a fully initialized excluded
        // sentinel exists. If marker persistence cannot be verified, leave no
        // marker behind; that installation can never be misclassified as a
        // restored backup on a later launch.
        if !markerExists {
            try persistMarker(
                at: markerURL,
                fileManager: fileManager,
                markerWriter: markerWriter
            )
        }

        return result
    }

    static func run(
        store: KeyValueStore,
        namespace: String,
        fileManager: FileManager = .default,
        sharedSentinelBaseURL: URL? = nil
    ) throws -> DetectionResult {
        try run(
            store: store,
            namespace: namespace,
            fileManager: fileManager,
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            )
        )
    }

    static func restoreResetIsRequired(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) -> Bool {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        // A malformed event is still recovery evidence. Treating unreadable
        // or empty contents as "no restore" would turn filesystem corruption
        // into permission to reuse restored tracking state.
        return fileManager.fileExists(
            atPath: restoreEventURL(sentinelURL: sentinelURL).path
        )
    }

    static func restoreResetEventIdentifier(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) -> String? {
        restoreResetEventIdentifier(
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            ),
            fileManager: fileManager
        )
    }

    static func restoreResetEventIdentifier(
        sentinelURL: URL,
        fileManager: FileManager = .default
    ) -> String? {
        let url = restoreEventURL(sentinelURL: sentinelURL)
        guard let data = try? Data(contentsOf: url),
              let value = String(data: data, encoding: .utf8),
              !value.isEmpty else {
            return nil
        }
        return value
    }

    /// The restore event is published before the new installation sentinel.
    /// Its filesystem timestamp is therefore a durable lower bound for local
    /// mutations made after restore recovery began, including mutations from
    /// another process sharing the same app-group Realm.
    static func restoreResetEventDate(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) -> Date? {
        restoreResetEventDate(
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            ),
            fileManager: fileManager
        )
    }

    static func restoreResetEventDate(
        sentinelURL: URL,
        fileManager: FileManager = .default
    ) -> Date? {
        let eventURL = restoreEventURL(sentinelURL: sentinelURL)
        guard let data = try? Data(contentsOf: eventURL),
              let value = String(data: data, encoding: .utf8),
              let encodedIdentifier = value.split(separator: "\n").first,
              UUID(uuidString: String(encodedIdentifier)) != nil else {
            return nil
        }
        if let encodedDate = value.split(separator: "\n").dropFirst().first,
           let interval = TimeInterval(encodedDate),
           interval.isFinite {
            return Date(timeIntervalSinceReferenceDate: interval)
        }
        // Accept an event created by the earlier v4 implementation during an
        // interrupted in-place upgrade. Its atomic file publication timestamp
        // is still a conservative recovery boundary.
        guard let attributes = try? fileManager.attributesOfItem(
            atPath: eventURL.path
        ),
        let date = attributes[.modificationDate] as? Date else {
            return nil
        }
        return date
    }

    /// Acknowledgement is scoped to one durable client. Other clients retain
    /// their own events even if they share a defaults store and sentinel base.
    static func markRestoreResetCompleted(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default,
        completionSynchronizer: ((URL) throws -> Void)? = nil
    ) throws {
        try markRestoreResetCompleted(
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            ),
            fileManager: fileManager,
            completionSynchronizer: completionSynchronizer
        )
    }

    static func markRestoreResetCompleted(
        sentinelURL: URL,
        fileManager: FileManager = .default,
        completionSynchronizer: ((URL) throws -> Void)? = nil
    ) throws {
        let url = restoreEventURL(sentinelURL: sentinelURL)
        guard fileManager.fileExists(atPath: url.path) else { return }
        guard let eventData = try? Data(contentsOf: url) else {
            throw Error.restoreEventAcknowledgementVerificationFailed
        }
        do {
            try fileManager.removeItem(at: url)
            if let completionSynchronizer {
                try completionSynchronizer(url)
            } else {
                try synchronizeParentDirectory(of: url)
            }
            guard !fileManager.fileExists(atPath: url.path) else {
                throw Error.restoreEventAcknowledgementVerificationFailed
            }
        } catch {
            // Best-effort restoration keeps this process fail-closed as well
            // as protecting the next launch when the removal cannot be proven
            // durable. The caller still receives the failure even if repair
            // succeeds, so it never reports an unverified acknowledgement.
            if !fileManager.fileExists(atPath: url.path) {
                try? eventData.write(to: url, options: .atomic)
                try? synchronizePublishedFile(at: url)
            }
            throw Error.restoreEventAcknowledgementVerificationFailed
        }
    }

    private static func createExcludedSentinel(
        at sentinelURL: URL,
        fileManager: FileManager
    ) throws {
        let parentURL = sentinelURL.deletingLastPathComponent()
        try fileManager.createDirectory(
            at: parentURL,
            withIntermediateDirectories: true
        )
        // Apply and verify the backup-exclusion attribute before the sentinel
        // becomes visible at its final path. An atomic same-directory rename
        // then publishes content + exclusion as one filesystem transition, so
        // a backup snapshot can never observe a final sentinel that still looks
        // backup-eligible.
        let temporaryURL = parentURL.appendingPathComponent(
            ".\(sentinelURL.lastPathComponent).\(UUID().uuidString).tmp"
        )
        defer { try? fileManager.removeItem(at: temporaryURL) }
        try Data("Backup detection file\n".utf8).write(
            to: temporaryURL,
            options: .atomic
        )
        try synchronizePublishedFile(at: temporaryURL)
        var resourceValues = URLResourceValues()
        resourceValues.isExcludedFromBackup = true
        var mutableTemporaryURL = temporaryURL
        try mutableTemporaryURL.setResourceValues(resourceValues)
        try synchronizeFile(at: temporaryURL)
        guard try temporaryURL.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup == true else {
            throw CocoaError(.fileWriteUnknown)
        }
        do {
            try fileManager.moveItem(at: temporaryURL, to: sentinelURL)
        } catch {
            // An app and extension sharing a client namespace may race their
            // first launch. Accept only a competing fully excluded sentinel.
            let publishedURL = URL(fileURLWithPath: sentinelURL.path)
            guard fileManager.fileExists(atPath: publishedURL.path),
                  try publishedURL.resourceValues(
                    forKeys: [.isExcludedFromBackupKey]
                  ).isExcludedFromBackup == true else {
                throw error
            }
        }
        let publishedURL = URL(fileURLWithPath: sentinelURL.path)
        guard try publishedURL.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup == true else {
            throw CocoaError(.fileWriteUnknown)
        }
        try synchronizePublishedFile(at: publishedURL)
    }

    private static func persistRestoreEvent(
        at url: URL,
        fileManager: FileManager,
        eventWriter: ((URL, Data) throws -> Void)?
    ) throws {
        // A restore-event file is intentionally backup eligible. If an
        // interrupted recovery is itself backed up and restored again, the
        // copied event describes the older installation and cannot delimit
        // the new installation's mutations. Every actual restore detection
        // (marker present, valid excluded sentinel absent) publishes a fresh
        // event before exposing the new sentinel. Ordinary crash resume keeps
        // the event because the already-published excluded sentinel makes the
        // next run a regular launch.
        let createdAt = Date()
        let data = Data(
            "\(UUID().uuidString)\n\(createdAt.timeIntervalSinceReferenceDate)"
                .utf8
        )
        try fileManager.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        do {
            if let eventWriter {
                try eventWriter(url, data)
            } else {
                try data.write(to: url, options: .atomic)
            }
            try synchronizePublishedFile(at: url)
        } catch {
            throw Error.restoreEventPersistenceVerificationFailed
        }
        guard let persisted = try? Data(contentsOf: url), persisted == data else {
            throw Error.restoreEventPersistenceVerificationFailed
        }
    }

    private static func persistMarker(
        at url: URL,
        fileManager: FileManager,
        markerWriter: ((URL, Data) throws -> Void)?
    ) throws {
        try fileManager.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        do {
            if let markerWriter {
                try markerWriter(url, markerData)
            } else {
                try markerData.write(to: url, options: .atomic)
            }
            try synchronizePublishedFile(at: url)
        } catch {
            try? fileManager.removeItem(at: url)
            throw Error.markerPersistenceVerificationFailed
        }
        guard persistedMarkerExists(at: url) else {
            // A custom writer can leave a partial marker behind. Remove only
            // this tracking file so it cannot become a false restore signal.
            try? fileManager.removeItem(at: url)
            throw Error.markerPersistenceVerificationFailed
        }
    }

    private static func persistedMarkerExists(at url: URL) -> Bool {
        (try? Data(contentsOf: url)) == markerData
    }
}
