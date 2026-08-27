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
    private static let sentinelHeader = "BigSyncKit installation v1"

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
        /// A caller attempted to continue a restore with a transaction other
        /// than the one durably recorded before identity publication.
        case manualRestoreTransactionMismatch
        /// A partial/corrupt handoff cannot prove which Realm installation
        /// owns the current sentinel. Leave it recoverable, but never guess.
        case manualRestoreStateAmbiguous
    }

    struct ManualRestoreReceipt: Equatable, Sendable {
        let transactionIdentifier: UUID
        let restoreEventIdentifier: UUID
        let oldInstallationIdentifier: String
        let newInstallationIdentifier: String
    }

    enum ManualRestorePreflight: Equatable, Sendable {
        case newTransaction
        /// Replacement may be incomplete and rollback is still permitted.
        case resumeIntent(ManualRestoreReceipt)
        /// The restore event is durable; rollback is no longer permitted.
        case resumeEvent(ManualRestoreReceipt)
        /// CloudKit already acknowledged the event, but the caller may have
        /// crashed before recording the returned receipt. The excluded local
        /// receipt makes that exact transaction permanently idempotent.
        case completed(ManualRestoreReceipt)
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
        try bigSyncSynchronizeDirectory(
            at: url.deletingLastPathComponent()
        )
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

    /// Written before the caller replaces any Realm file. Its presence fences
    /// peer processes even if the replacing process dies before it can publish
    /// the restore event and rotate the installation sentinel.
    static func manualRestoreIntentURL(sentinelURL: URL) -> URL {
        sentinelURL.appendingPathExtension("restore-intent")
    }

    /// Backup-excluded proof retained after CloudKit acknowledges the restore
    /// event. Without it, a caller crash between BigSync returning and its own
    /// journal commit could later look like a brand-new restore after a peer
    /// process consumes the event.
    static func completedManualRestoreReceiptURL(sentinelURL: URL) -> URL {
        sentinelURL.appendingPathExtension("manual-restore-receipt")
    }

    private static func restoreEventLockURL(sentinelURL: URL) -> URL {
        sentinelURL.appendingPathExtension("restore-event-lock")
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

    static func manualRestoreReceipt(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil
    ) -> ManualRestoreReceipt? {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        return manualRestoreReceipt(
            at: restoreEventURL(sentinelURL: sentinelURL)
        )
    }

    static func manualRestoreIntentIsRequired(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) -> Bool {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        return fileManager.fileExists(
            atPath: manualRestoreIntentURL(sentinelURL: sentinelURL).path
        )
    }

    static func manualRestoreIntentReceipt(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil
    ) -> ManualRestoreReceipt? {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        return manualRestoreReceipt(
            at: manualRestoreIntentURL(sentinelURL: sentinelURL)
        )
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
        try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            try runLocked(
                store: store,
                namespace: namespace,
                fileManager: fileManager,
                sentinelURL: sentinelURL,
                eventWriter: eventWriter,
                markerWriter: markerWriter
            )
        }
    }

    private static func runLocked(
        store: KeyValueStore,
        namespace: String,
        fileManager: FileManager,
        sentinelURL: URL,
        eventWriter: ((URL, Data) throws -> Void)?,
        markerWriter: ((URL, Data) throws -> Void)?
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
                try replaceExcludedSentinel(
                    at: sentinelURL,
                    fileManager: fileManager
                )
            } else {
                try createExcludedSentinel(
                    at: sentinelURL,
                    fileManager: fileManager
                )
            }
        } else if installationIdentifier(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) == nil {
            // Cleanly upgrade the old constant-content sentinel. It is
            // sync-only, backup-excluded state, so replacing it cannot alter
            // user data or fabricate a restore event.
            try replaceExcludedSentinel(
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

    static func installationIdentifier(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) -> String? {
        installationIdentifier(
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            ),
            fileManager: fileManager
        )
    }

    static func installationIdentifier(
        sentinelURL: URL,
        fileManager: FileManager = .default
    ) -> String? {
        guard fileManager.fileExists(atPath: sentinelURL.path),
              let data = try? Data(contentsOf: sentinelURL),
              let value = String(data: data, encoding: .utf8) else {
            return nil
        }
        let lines = value.split(separator: "\n")
        guard lines.first.map(String.init) == sentinelHeader,
              lines.count >= 2,
              let identifier = UUID(uuidString: String(lines[1])) else {
            return nil
        }
        return identifier.uuidString.lowercased()
    }

    static func beginManualRestore(
        namespace: String,
        transactionIdentifier: UUID,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default,
        sentinelPublisher: ((URL, FileManager) throws -> Void)? = nil
    ) throws -> ManualRestoreReceipt {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        BigSyncClientIdentityLeaseRegistry.invalidateInstallationIdentifier(
            at: sentinelURL.appendingPathExtension("lease")
        )
        return try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            if case .completed(let receipt) = try manualRestorePreflightLocked(
                transactionIdentifier: transactionIdentifier,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            ) {
                return receipt
            }
            let receipt = try prepareManualRestoreIntentLocked(
                transactionIdentifier: transactionIdentifier,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
            let eventURL = restoreEventURL(sentinelURL: sentinelURL)
            if !fileManager.fileExists(atPath: eventURL.path) {
                try persistManualRestoreEvent(
                    receipt,
                    at: eventURL,
                    fileManager: fileManager
                )
            }

            let currentInstallationIdentifier = installationIdentifier(
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
            if currentInstallationIdentifier != receipt.newInstallationIdentifier {
                guard currentInstallationIdentifier
                        == receipt.oldInstallationIdentifier else {
                    throw Error.manualRestoreStateAmbiguous
                }
                if let sentinelPublisher {
                    try sentinelPublisher(sentinelURL, fileManager)
                } else {
                    try replaceExcludedSentinel(
                        at: sentinelURL,
                        fileManager: fileManager,
                        data: sentinelData(
                            installationIdentifier:
                                receipt.newInstallationIdentifier
                        )
                    )
                }
            }
            guard installationIdentifier(
                sentinelURL: sentinelURL,
                fileManager: fileManager
            ) == receipt.newInstallationIdentifier else {
                throw Error.manualRestoreStateAmbiguous
            }
            try persistManualRestoreReceiptExcludingBackup(
                receipt,
                at: completedManualRestoreReceiptURL(
                    sentinelURL: sentinelURL
                ),
                fileManager: fileManager
            )
            try removeManualRestoreIntentLocked(
                receipt,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
            return receipt
        }
    }

    /// Publishes the cross-process fence before a caller touches Realm files.
    static func prepareManualRestoreIntent(
        namespace: String,
        transactionIdentifier: UUID,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) throws -> ManualRestoreReceipt {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        BigSyncClientIdentityLeaseRegistry.invalidateInstallationIdentifier(
            at: sentinelURL.appendingPathExtension("lease")
        )
        return try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            try prepareManualRestoreIntentLocked(
                transactionIdentifier: transactionIdentifier,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
        }
    }

    static func cancelManualRestoreIntent(
        namespace: String,
        receipt: ManualRestoreReceipt,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) throws {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            guard !fileManager.fileExists(
                atPath: restoreEventURL(sentinelURL: sentinelURL).path
            ), installationIdentifier(
                sentinelURL: sentinelURL,
                fileManager: fileManager
            ) == receipt.oldInstallationIdentifier else {
                throw Error.manualRestoreStateAmbiguous
            }
            try removeManualRestoreIntentLocked(
                receipt,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
        }
    }

    /// Inspects the durable intent, event, and last completed receipt. Call
    /// this while holding the client lease before touching a target Realm: a
    /// matching record proves which phase owns the replacement, while a
    /// mismatch must fail before another replacement can begin.
    static func manualRestorePreflight(
        namespace: String,
        transactionIdentifier: UUID,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default
    ) throws -> ManualRestorePreflight {
        let sentinelURL = defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: sharedSentinelBaseURL
        )
        return try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            try manualRestorePreflightLocked(
                transactionIdentifier: transactionIdentifier,
                sentinelURL: sentinelURL,
                fileManager: fileManager
            )
        }
    }

    private static func manualRestorePreflightLocked(
        transactionIdentifier: UUID,
        sentinelURL: URL,
        fileManager: FileManager
    ) throws -> ManualRestorePreflight {
        let intentURL = manualRestoreIntentURL(sentinelURL: sentinelURL)
        let eventURL = restoreEventURL(sentinelURL: sentinelURL)
        let intentExists = fileManager.fileExists(atPath: intentURL.path)
        let eventExists = fileManager.fileExists(atPath: eventURL.path)
        let completedURL = completedManualRestoreReceiptURL(
            sentinelURL: sentinelURL
        )
        let completedExists = fileManager.fileExists(atPath: completedURL.path)
        let completed: ManualRestoreReceipt?
        if completedExists {
            guard let receipt = manualRestoreReceipt(at: completedURL),
                  installationIdentifier(
                    sentinelURL: sentinelURL,
                    fileManager: fileManager
                  ) == receipt.newInstallationIdentifier else {
                throw Error.manualRestoreStateAmbiguous
            }
            completed = receipt
        } else {
            completed = nil
        }

        // A matching completion receipt proves the replacement finished.
        // Matching intent/event files are residue from cleanup interrupted by
        // a crash; conflicting or unreadable residue remains ambiguous.
        if let completed,
           completed.transactionIdentifier == transactionIdentifier {
            let intent = intentExists ? manualRestoreReceipt(at: intentURL) : nil
            let event = eventExists ? manualRestoreReceipt(at: eventURL) : nil
            if (intentExists && intent == nil)
                || (eventExists && event == nil)
                || intent.map({ $0 != completed }) == true
                || event.map({ $0 != completed }) == true {
                throw Error.manualRestoreStateAmbiguous
            }
            return .completed(completed)
        }

        if !intentExists && !eventExists {
            return .newTransaction
        }

        let intent = intentExists ? manualRestoreReceipt(at: intentURL) : nil
        let event = eventExists ? manualRestoreReceipt(at: eventURL) : nil
        if (intentExists && intent == nil)
            || (eventExists && event == nil) {
            throw Error.manualRestoreStateAmbiguous
        }
        if let intent, let event, intent != event {
            throw Error.manualRestoreStateAmbiguous
        }
        guard let receipt = event ?? intent else {
            throw Error.manualRestoreStateAmbiguous
        }
        guard receipt.transactionIdentifier == transactionIdentifier else {
            throw Error.manualRestoreTransactionMismatch
        }
        return event != nil ? .resumeEvent(receipt) : .resumeIntent(receipt)
    }

    private static func prepareManualRestoreIntentLocked(
        transactionIdentifier: UUID,
        sentinelURL: URL,
        fileManager: FileManager
    ) throws -> ManualRestoreReceipt {
        let receipt: ManualRestoreReceipt
        switch try manualRestorePreflightLocked(
            transactionIdentifier: transactionIdentifier,
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
        case .resumeIntent(let existing), .resumeEvent(let existing),
             .completed(let existing):
            receipt = existing
        case .newTransaction:
            guard let oldInstallationIdentifier = installationIdentifier(
                sentinelURL: sentinelURL,
                fileManager: fileManager
            ) else {
                throw Error.manualRestoreStateAmbiguous
            }
            receipt = ManualRestoreReceipt(
                transactionIdentifier: transactionIdentifier,
                restoreEventIdentifier: UUID(),
                oldInstallationIdentifier: oldInstallationIdentifier,
                newInstallationIdentifier: UUID().uuidString.lowercased()
            )
            try persistManualRestoreIntent(
                receipt,
                at: manualRestoreIntentURL(sentinelURL: sentinelURL),
                fileManager: fileManager
            )
        }
        return receipt
    }

    private static func removeManualRestoreIntentLocked(
        _ receipt: ManualRestoreReceipt,
        sentinelURL: URL,
        fileManager: FileManager
    ) throws {
        let intentURL = manualRestoreIntentURL(sentinelURL: sentinelURL)
        guard fileManager.fileExists(atPath: intentURL.path) else { return }
        guard manualRestoreReceipt(at: intentURL) == receipt else {
            throw Error.manualRestoreStateAmbiguous
        }
        try fileManager.removeItem(at: intentURL)
        try synchronizeParentDirectory(of: intentURL)
        guard !fileManager.fileExists(atPath: intentURL.path) else {
            throw Error.manualRestoreStateAmbiguous
        }
    }

    /// Compatibility entry point for internal callers that do not have a
    /// caller journal. New restore coordinators must supply their own stable
    /// transaction identifier and retain the full receipt.
    static func beginManualRestore(
        namespace: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default,
        sentinelPublisher: ((URL, FileManager) throws -> Void)? = nil
    ) throws -> String {
        try beginManualRestore(
            namespace: namespace,
            transactionIdentifier: UUID(),
            sharedSentinelBaseURL: sharedSentinelBaseURL,
            fileManager: fileManager,
            sentinelPublisher: sentinelPublisher
        ).newInstallationIdentifier
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
              let value = String(data: data, encoding: .utf8) else {
            return nil
        }
        let lines = value.split(separator: "\n").map(String.init)
        if lines.first == "BigSyncKit manual restore v1",
           lines.count >= 3,
           let identifier = UUID(uuidString: lines[2]) {
            return identifier.uuidString.lowercased()
        }
        guard let firstLine = lines.first,
              let identifier = UUID(uuidString: firstLine) else { return nil }
        return identifier.uuidString.lowercased()
    }

    /// Acknowledgement is scoped to one durable client. Other clients retain
    /// their own events even if they share a defaults store and sentinel base.
    static func markRestoreResetCompleted(
        namespace: String,
        expectedEventIdentifier: String,
        sharedSentinelBaseURL: URL? = nil,
        fileManager: FileManager = .default,
        completionSynchronizer: ((URL) throws -> Void)? = nil
    ) throws {
        try markRestoreResetCompleted(
            sentinelURL: defaultSentinelURL(
                namespace: namespace,
                sharedBaseURL: sharedSentinelBaseURL
            ),
            expectedEventIdentifier: expectedEventIdentifier,
            fileManager: fileManager,
            completionSynchronizer: completionSynchronizer
        )
    }

    static func markRestoreResetCompleted(
        sentinelURL: URL,
        expectedEventIdentifier: String,
        fileManager: FileManager = .default,
        completionSynchronizer: ((URL) throws -> Void)? = nil
    ) throws {
        try withRestoreStateLock(
            sentinelURL: sentinelURL,
            fileManager: fileManager
        ) {
            let url = restoreEventURL(sentinelURL: sentinelURL)
            guard fileManager.fileExists(atPath: url.path) else { return }
            guard let eventData = try? Data(contentsOf: url),
                  restoreResetEventIdentifier(
                    sentinelURL: sentinelURL,
                    fileManager: fileManager
                  ) == expectedEventIdentifier else {
                throw Error.restoreEventAcknowledgementVerificationFailed
            }
            do {
                // Retire the optional pre-replacement intent first. A crash
                // after this point still leaves the authoritative recovery
                // event in place, so a later synchronization must reconcile
                // CloudKit again before acknowledging that exact event. The
                // inverse order can strand an intent with no event and no
                // owning restore coordinator left to resume it.
                if let intent = manualRestoreReceipt(
                    at: manualRestoreIntentURL(sentinelURL: sentinelURL)
                ), intent.restoreEventIdentifier.uuidString.lowercased()
                    == expectedEventIdentifier {
                    try removeManualRestoreIntentLocked(
                        intent,
                        sentinelURL: sentinelURL,
                        fileManager: fileManager
                    )
                }
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
                // The common event lock prevents a newer writer from being
                // overwritten by this repair. Restore only the exact event
                // whose acknowledgement failed.
                if !fileManager.fileExists(atPath: url.path) {
                    try? eventData.write(to: url, options: .atomic)
                    try? synchronizePublishedFile(at: url)
                }
                throw Error.restoreEventAcknowledgementVerificationFailed
            }
        }
    }

    private static func createExcludedSentinel(
        at sentinelURL: URL,
        fileManager: FileManager,
        data: Data? = nil
    ) throws {
        let parentURL = sentinelURL.deletingLastPathComponent()
        try bigSyncCreateDirectoryDurably(
            at: parentURL,
            fileManager: fileManager
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
        let sentinelData = data ?? Data(
            "\(sentinelHeader)\n\(UUID().uuidString.lowercased())\n".utf8
        )
        try sentinelData.write(
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

    /// Atomically rotates an existing installation sentinel. The replacement
    /// receives and verifies its backup-exclusion attribute before the POSIX
    /// rename, so another app-group process never observes a missing identity
    /// or a final-path sentinel that is temporarily backup eligible.
    private static func replaceExcludedSentinel(
        at sentinelURL: URL,
        fileManager: FileManager,
        data: Data? = nil
    ) throws {
        let parentURL = sentinelURL.deletingLastPathComponent()
        try bigSyncCreateDirectoryDurably(
            at: parentURL,
            fileManager: fileManager
        )
        let temporaryURL = parentURL.appendingPathComponent(
            ".\(sentinelURL.lastPathComponent).\(UUID().uuidString).replacement"
        )
        defer { try? fileManager.removeItem(at: temporaryURL) }
        let sentinelData = data ?? Data(
            "\(sentinelHeader)\n\(UUID().uuidString.lowercased())\n".utf8
        )
        try sentinelData.write(to: temporaryURL, options: .atomic)
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
        guard Darwin.rename(temporaryURL.path, sentinelURL.path) == 0 else {
            throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
        }
        let publishedURL = URL(fileURLWithPath: sentinelURL.path)
        try synchronizePublishedFile(at: publishedURL)
        guard try publishedURL.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup == true,
        try Data(contentsOf: publishedURL) == sentinelData else {
            throw CocoaError(.fileWriteUnknown)
        }
    }

    private static func persistRestoreEvent(
        at url: URL,
        fileManager: FileManager,
        eventWriter: ((URL, Data) throws -> Void)?
    ) throws {
        // A restore-event file is intentionally backup eligible. If an
        // interrupted recovery is itself backed up and restored again, the
        // copied UUID describes the older installation and cannot acknowledge
        // the new installation's recovery. Every actual restore detection
        // (marker present, valid excluded sentinel absent) publishes a fresh
        // event before exposing the new sentinel. Ordinary crash resume keeps
        // the event because the already-published excluded sentinel makes the
        // next run a regular launch.
        let data = Data("\(UUID().uuidString.lowercased())\n".utf8)
        try bigSyncCreateDirectoryDurably(
            at: url.deletingLastPathComponent(),
            fileManager: fileManager
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

    private static func sentinelData(installationIdentifier: String) -> Data {
        Data("\(sentinelHeader)\n\(installationIdentifier)\n".utf8)
    }

    private static func withRestoreStateLock<T>(
        sentinelURL: URL,
        fileManager: FileManager,
        _ operation: () throws -> T
    ) throws -> T {
        let lockURL = restoreEventLockURL(sentinelURL: sentinelURL)
        try bigSyncCreateDirectoryDurably(
            at: lockURL.deletingLastPathComponent(),
            fileManager: fileManager
        )
        let descriptor = Darwin.open(
            lockURL.path,
            O_CREAT | O_RDWR,
            S_IRUSR | S_IWUSR
        )
        guard descriptor >= 0 else {
            throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
        }
        defer { Darwin.close(descriptor) }
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        var mutableLockURL = lockURL
        try mutableLockURL.setResourceValues(values)
        guard bigSyncFlock(descriptor, LOCK_EX) == 0 else {
            throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
        }
        defer { _ = bigSyncFlock(descriptor, LOCK_UN) }
        return try operation()
    }

    static func manualRestoreReceipt(at url: URL) -> ManualRestoreReceipt? {
        guard let data = try? Data(contentsOf: url),
              let value = String(data: data, encoding: .utf8) else { return nil }
        let lines = value.split(separator: "\n").map(String.init)
        guard lines.count == 6,
              lines[0] == "BigSyncKit manual restore v1",
              let transactionIdentifier = UUID(uuidString: lines[1]),
              let restoreEventIdentifier = UUID(uuidString: lines[2]),
              let oldInstallationIdentifier = UUID(uuidString: lines[3]),
              let newInstallationIdentifier = UUID(uuidString: lines[4]),
              lines[5] == "end" else { return nil }
        return ManualRestoreReceipt(
            transactionIdentifier: transactionIdentifier,
            restoreEventIdentifier: restoreEventIdentifier,
            oldInstallationIdentifier: oldInstallationIdentifier.uuidString.lowercased(),
            newInstallationIdentifier: newInstallationIdentifier.uuidString.lowercased()
        )
    }

    private static func persistManualRestoreIntent(
        _ receipt: ManualRestoreReceipt,
        at url: URL,
        fileManager: FileManager
    ) throws {
        try persistManualRestoreReceiptExcludingBackup(
            receipt,
            at: url,
            fileManager: fileManager
        )
    }

    private static func persistManualRestoreReceiptExcludingBackup(
        _ receipt: ManualRestoreReceipt,
        at url: URL,
        fileManager: FileManager
    ) throws {
        let data = manualRestoreData(receipt)
        let directory = url.deletingLastPathComponent()
        try bigSyncCreateDirectoryDurably(
            at: directory,
            fileManager: fileManager
        )
        let temporaryURL = directory.appendingPathComponent(
            ".\(url.lastPathComponent).\(UUID().uuidString).tmp"
        )
        defer { try? fileManager.removeItem(at: temporaryURL) }
        try data.write(to: temporaryURL, options: .atomic)
        try synchronizePublishedFile(at: temporaryURL)
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        var mutableTemporaryURL = temporaryURL
        try mutableTemporaryURL.setResourceValues(values)
        try synchronizeFile(at: temporaryURL)
        guard try temporaryURL.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup == true,
        Darwin.rename(temporaryURL.path, url.path) == 0 else {
            throw Error.restoreEventPersistenceVerificationFailed
        }
        try synchronizePublishedFile(at: url)
        guard manualRestoreReceipt(at: url) == receipt,
              try url.resourceValues(
                forKeys: [.isExcludedFromBackupKey]
              ).isExcludedFromBackup == true else {
            throw Error.restoreEventPersistenceVerificationFailed
        }
    }

    private static func persistManualRestoreEvent(
        _ receipt: ManualRestoreReceipt,
        at url: URL,
        fileManager: FileManager
    ) throws {
        let data = manualRestoreData(receipt)
        let directory = url.deletingLastPathComponent()
        try bigSyncCreateDirectoryDurably(
            at: directory,
            fileManager: fileManager
        )
        let temporaryURL = directory.appendingPathComponent(
            ".\(url.lastPathComponent).\(UUID().uuidString).tmp"
        )
        defer { try? fileManager.removeItem(at: temporaryURL) }
        do {
            try data.write(to: temporaryURL, options: .withoutOverwriting)
            try synchronizeFile(at: temporaryURL)
            guard Darwin.rename(temporaryURL.path, url.path) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            try synchronizePublishedFile(at: url)
        } catch {
            throw Error.restoreEventPersistenceVerificationFailed
        }
        guard (try? Data(contentsOf: url)) == data else {
            throw Error.restoreEventPersistenceVerificationFailed
        }
    }

    private static func manualRestoreData(
        _ receipt: ManualRestoreReceipt
    ) -> Data {
        Data(
            "BigSyncKit manual restore v1\n\(receipt.transactionIdentifier.uuidString.lowercased())\n\(receipt.restoreEventIdentifier.uuidString.lowercased())\n\(receipt.oldInstallationIdentifier)\n\(receipt.newInstallationIdentifier)\nend\n".utf8
        )
    }

    private static func persistMarker(
        at url: URL,
        fileManager: FileManager,
        markerWriter: ((URL, Data) throws -> Void)?
    ) throws {
        try bigSyncCreateDirectoryDurably(
            at: url.deletingLastPathComponent(),
            fileManager: fileManager
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
