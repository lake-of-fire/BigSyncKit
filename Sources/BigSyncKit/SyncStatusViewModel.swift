import SwiftUI
import RealmSwift
import RealmSwiftGaps
import CloudKit
import Combine

public struct LastSeenDevice: Identifiable {
    public let id: UUID
    public let deviceName: String
    public let lastSeenOnline: Date
    public let humanReadableLastSeenOnline: String
    
    public init(id: UUID, deviceName: String, lastSeenOnline: Date) {
        self.id = id
        self.deviceName = deviceName
        self.lastSeenOnline = lastSeenOnline
        
        let interval = Calendar.current.dateComponents([.year, .month, .day, .hour, .minute, .nanosecond], from: lastSeenOnline, to: Date())
        let intervalText: String
        if let year = interval.year, year > 0 {
            intervalText = "\(year) year\(year != 1 ? "s" : "")"
        } else if let month = interval.month, month > 0 {
            intervalText = "\(month) month\(month != 1 ? "s" : "")"
        } else if let day = interval.day, day > 0 {
            intervalText = "\(day) day\(day != 1 ? "s" : "")"
        } else if let hour = interval.hour, hour > 0 {
            intervalText = "\(hour) hour\(hour != 1 ? "s" : "")"
        } else if let minute = interval.minute, minute > 0 {
            intervalText = "\(minute) minute\(minute != 1 ? "s" : "")"
        } else if let nanosecond = interval.nanosecond, nanosecond > 0 {
            intervalText = "\(nanosecond / 1000000000) second\(nanosecond != 1000000000 ? "s" : "")"
        } else {
            intervalText = ""
        }
        humanReadableLastSeenOnline = "\(intervalText) ago"
    }
}

@MainActor
public class SyncStatusViewModel: ObservableObject {
    public let realmConfiguration: Realm.Configuration
    
    @Published public var syncStatus: String = "Initializing"
//    @Published public var syncStatusWithoutFailure: String = "Initializing"
    @Published public var syncFailed = false
    @Published public var currentDeviceID: UUID?
    /// Durable, account-scoped state restored independently from the transient
    /// notification-driven status above.
    @Published public private(set) var cloudKitSyncHealth: CloudKitSyncHealthSnapshot?
    /// A durable account change that requires an explicit dataset port before
    /// CloudKit synchronization may resume. BigSyncKit only surfaces the gate;
    /// the host application owns any future confirmation and port workflow.
    @Published public private(set) var cloudAccountPortRequirement:
        BigSyncCloudAccountPortRequirement?
//    @Published public var lastSeenDevices: [LastSeenDevice]?
    @Published public var changesRemainingToUpload: Int?

    private var cancellables = Set<AnyCancellable>()
    private var cloudKitHealthRefreshTask: Task<Void, Never>?
    private var cloudKitHealthRefreshGeneration = 0
#if DEBUG
    var _testCloudKitSyncHealthSnapshotProvider:
        (@MainActor @Sendable () async -> CloudKitSyncHealthSnapshot?)?
#endif
    
    public init(realmConfiguration: Realm.Configuration) {
        self.realmConfiguration = realmConfiguration
        
        NotificationCenter.default.publisher(for: .SynchronizerChangesRemainingToUpload)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] notification in
                guard let self else { return }
                let userInfo = notification.userInfo
                if let count = userInfo?["CloudKitSynchronizerChangesRemainingToUploadKey"] as? Int {
                    changesRemainingToUpload = count
                }
            }
            .store(in: &cancellables)

        NotificationCenter.default.publisher(for: .SynchronizerSyncHealthDidChange)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] _ in
                // Treat notifications as wakeups. Their payload can belong to
                // a synchronizer/account that has already been replaced.
                self?.reloadCloudKitSyncHealth()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillSynchronize)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] _ in
                guard let self else { return }
                syncStatus = "Preparing to Synchronize"
                syncFailed = false
                syncBegan()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillFetchChanges)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] _ in
                guard let self else { return }
                syncStatus = "Fetching Changes"
                syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillUploadChanges)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] _ in
                guard let self else { return }
                syncStatus = "Uploading Changes"
                syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidSynchronize)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] _ in
                guard let self else { return }
                if changesRemainingToUpload ?? 0 > 0 {
                    syncStatus = "Partial Synchronization Completed"
                } else {
                    syncStatus = "Synchronization Completed"
                }
                refreshCloudAccountPortRequirement()
                syncFailed = false
                syncIsOver()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidFailToSynchronize)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] notification in
                guard let self else { return }
                applySynchronizationFailure(
                    notification.userInfo?[cloudKitSynchronizerErrorKey]
                        as? Error
                )
                syncIsOver()
            }
            .store(in: &cancellables)
    }

    /// Reload after the BigSync worker is configured. The worker verifies the
    /// current account scope before returning persisted state.
    public func restoreCloudKitSyncHealth() {
        reloadCloudKitSyncHealth()
        Task { @MainActor [weak self] in
            let portRequirement = try? await BigSyncBackgroundActor.shared
                .pendingCloudAccountPortRequirement()
            self?.cloudAccountPortRequirement = portRequirement
        }
    }

    private func reloadCloudKitSyncHealth() {
        cloudKitHealthRefreshGeneration &+= 1
        let generation = cloudKitHealthRefreshGeneration
        cloudKitHealthRefreshTask?.cancel()
        cloudKitHealthRefreshTask = Task { @MainActor [weak self] in
            let snapshot: CloudKitSyncHealthSnapshot?
#if DEBUG
            if let provider = self?._testCloudKitSyncHealthSnapshotProvider {
                snapshot = await provider()
            } else {
                snapshot = await BigSyncBackgroundActor.shared
                    .cloudKitSyncHealthSnapshot()
            }
#else
            snapshot = await BigSyncBackgroundActor.shared
                .cloudKitSyncHealthSnapshot()
#endif
            guard !Task.isCancelled,
                  let self,
                  cloudKitHealthRefreshGeneration == generation else {
                return
            }
            cloudKitSyncHealth = snapshot
            cloudKitHealthRefreshTask = nil
        }
    }

    private func refreshCloudAccountPortRequirement() {
        Task { @MainActor [weak self] in
            self?.cloudAccountPortRequirement = try? await
                BigSyncBackgroundActor.shared
                    .pendingCloudAccountPortRequirement()
        }
    }

    public var cloudKitSyncHealthText: String? {
        if cloudAccountPortRequirement != nil {
            return "Your iCloud account changed; Manabi data must be moved before sync can resume"
        }
        guard let cloudKitSyncHealth else { return nil }
        switch cloudKitSyncHealth.terminalZoneDeletionKind {
        case .purged:
            return "iCloud data was removed in Settings; sync is paused and local data will not be re-uploaded"
        case .encryptedDataReset:
            if cloudKitSyncHealth.category == .succeeded {
                break
            }
            return "Recovering iCloud data after an encrypted-data reset"
        case .deleted:
            return "The iCloud sync zone was deleted; local data was preserved and sync is paused"
        case .unknown:
            return "The iCloud sync zone is unavailable; local data was preserved and sync is paused"
        case nil:
            break
        }
        switch cloudKitSyncHealth.category {
        case .idle: return "Idle"
        case .syncing: return "Synchronizing"
        case .succeeded: return "Last sync succeeded"
        case .semanticBlocked:
            return "Cloud transport is current; application recovery is incomplete"
        case .transientRetry:
            if let retryNotBefore = cloudKitSyncHealth.retryNotBefore {
                return "Retrying after \(retryNotBefore.formatted(date: .abbreviated, time: .shortened))"
            }
            return "Retrying"
        case .notAuthenticated: return "iCloud authentication required"
        case .accountTemporarilyUnavailable:
            return "iCloud account is temporarily unavailable"
        case .higherModelVersion: return "Update required to sync newer data"
        case .terminalZoneUnavailable: return "Cloud sync recovery required"
        case .failed: return "Last sync failed"
        }
    }

    func applySynchronizationFailure(_ error: Error?) {
        var didFail = true
        if let error {
            if let portError = error as? BigSyncCloudAccountPortError {
                switch portError {
                case .required(let requirement):
                    cloudAccountPortRequirement = requirement
                    syncStatus = "iCloud Account Change Requires Data Transfer"
                case .corruptRequirement:
                    cloudAccountPortRequirement = nil
                    syncStatus = "iCloud Account Change Requires Recovery"
                case .initialDatasetAdmissionUnavailable:
                    cloudAccountPortRequirement = nil
                    syncStatus = "iCloud Dataset Admission Unavailable"
                case .workerRestartRequired:
                    cloudAccountPortRequirement = nil
                    syncStatus = "Restarting iCloud Synchronization"
                    didFail = false
                }
            } else if let error = error as? CKError {
                switch error.code {
                case .changeTokenExpired:
                    syncStatus = "Reloading Synchronization"
                    didFail = false
                case .accountTemporarilyUnavailable:
                    syncStatus = "Account Temporarily Unavailable"
                case .constraintViolation:
                    syncStatus = "Synchronization Failed: Constraint Violation"
                case .limitExceeded:
                    // It restarts...
                    didFail = false
                default:
                    syncStatus = "Synchronization Failed: \(String(describing: error).prefix(150))"
                }
            } else if error is CancellationError {
                didFail = false
            } else {
                syncStatus = "Synchronization Failed: \(error.localizedDescription)"
            }
        } else {
            syncStatus = "Synchronization Failed: Unknown Error"
        }
        syncFailed = didFail
    }
    
    private func syncBegan() {
//        guard let currentDeviceID = currentDeviceID else { return }
//        Task(priority: .background) { @RealmBackgroundActor in
//            try await SyncedDevice.updateLastSeenOnlineIfNeeded(forUUID: currentDeviceID, realmConfiguration: realmConfiguration)
//            try await refreshLastSeenDevices()
//        }
    }
    
    private func syncIsOver() {
//        guard let currentDeviceID = currentDeviceID else { return }
//        Task(priority: .background) { @RealmBackgroundActor in
//            try await SyncedDevice.updateLastSeenOnlineIfNeeded(forUUID: currentDeviceID, realmConfiguration: realmConfiguration)
//            try await refreshLastSeenDevices()
//        }
    }
    
//    @RealmBackgroundActor
//    private func refreshLastSeenDevices() async throws {
//        let realm = try await Realm(configuration: realmConfiguration, actor: RealmBackgroundActor.shared)
//        let syncedDevices = realm.objects(SyncedDevice.self)
//            .where { !$0.isDeleted }
//            .sorted(by: \.lastSeenOnline, ascending: false)
//        lastSeenDevices = Array(syncedDevices).map {
//            LastSeenDevice(
//                id: $0.id, deviceName: $0.deviceName,
//                lastSeenOnline: $0.lastSeenOnline
//            )
//        }
//    }
}
