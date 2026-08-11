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
//    @Published public var lastSeenDevices: [LastSeenDevice]?
    @Published public var changesRemainingToUpload: Int?

    private var cancellables = Set<AnyCancellable>()
    
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
            .sink { @Sendable @MainActor [weak self] notification in
                guard let self,
                      let snapshot = notification.userInfo?[
                        cloudKitSynchronizerSyncHealthSnapshotKey
                      ] as? CloudKitSyncHealthSnapshot else { return }
                cloudKitSyncHealth = snapshot
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
                syncFailed = false
                syncIsOver()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidFailToSynchronize)
            .receive(on: RunLoop.main)
            .sink { @Sendable @MainActor [weak self] notification in
                guard let self else { return }
                let userInfo = notification.userInfo
                var syncFailed = true
                if let error = userInfo?[cloudKitSynchronizerErrorKey] as? Error {
                    if let error = error as? CKError {
                        switch error.code {
                        case .changeTokenExpired:
                            syncStatus = "Reloading Synchronization"
                            syncFailed = false
                        case .accountTemporarilyUnavailable:
                            syncStatus = "Account Temporarily Unavailable"
                        case .constraintViolation:
                            syncStatus = "Synchronization Failed: Constraint Violation"
                        case .limitExceeded:
                            // It restarts...
                            syncFailed = false
                        default:
                            syncStatus = "Synchronization Failed: \(String(describing: error).prefix(150))"
                        }
                    } else if error is CancellationError {
                        syncFailed = false
                    } else {
                        syncStatus = "Synchronization Failed: \(error.localizedDescription)"
                    }
                } else {
                    syncStatus = "Synchronization Failed: Unknown Error"
                }
                self.syncFailed = syncFailed
                syncIsOver()
            }
            .store(in: &cancellables)
    }

    /// Reload after the BigSync worker is configured. The worker verifies the
    /// current account scope before returning persisted state.
    public func restoreCloudKitSyncHealth() {
        Task { @MainActor [weak self] in
            let snapshot = await BigSyncBackgroundActor.shared
                .cloudKitSyncHealthSnapshot()
            self?.cloudKitSyncHealth = snapshot
        }
    }

    public var cloudKitSyncHealthText: String? {
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
        case .transientRetry:
            if let retryNotBefore = cloudKitSyncHealth.retryNotBefore {
                return "Retrying after \(retryNotBefore.formatted(date: .abbreviated, time: .shortened))"
            }
            return "Retrying"
        case .notAuthenticated: return "iCloud authentication required"
        case .higherModelVersion: return "Update required to sync newer data"
        case .terminalZoneUnavailable: return "Cloud sync recovery required"
        case .failed: return "Last sync failed"
        }
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
