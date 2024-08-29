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

public class SyncStatusViewModel: ObservableObject {
    public let realmConfiguration: Realm.Configuration
    
    @Published public var syncStatus: String = "Initializing"
//    @Published public var syncStatusWithoutFailure: String = "Initializing"
    @Published public var syncFailed = false
    @Published public var currentDeviceID: UUID?
    @Published public var lastSeenDevices: [LastSeenDevice]?
    
    public var bigSyncBackgroundWorker: BigSyncBackgroundWorker?

    private var cancellables = Set<AnyCancellable>()
    
    public init(realmConfiguration: Realm.Configuration) {
        self.realmConfiguration = realmConfiguration
        
        NotificationCenter.default.publisher(for: .SynchronizerWillSynchronize)
            .sink { [weak self] _ in
                guard let self = self else { return }
                syncStatus = "Preparing to Synchronize"
                syncFailed = false
                syncBegan()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillFetchChanges)
            .sink { [weak self] _ in
                guard let self = self else { return }
                syncStatus = "Fetching Changes"
                syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillUploadChanges)
            .sink { [weak self] _ in
                guard let self = self else { return }
                syncStatus = "Uploading Changes"
                syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidSynchronize)
            .sink { [weak self] _ in
                guard let self = self else { return }
                syncStatus = "Synchronization Completed"
                syncFailed = false
                syncIsOver()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidFailToSynchronize)
            .subscribe(on: DispatchQueue.main)
            .sink { [weak self] notification in
                let userInfo = notification.userInfo
                Task { @MainActor [weak self] in
                    guard let self = self else { return }
                    var syncFailed = true
                    if let error = userInfo?[CloudKitSynchronizer.errorKey] as? Error {
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
                        } else if let cancellationError = error as? CancellationError {
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
            }
            .store(in: &cancellables)
    }
    
    private func syncBegan() {
//        guard let currentDeviceID = currentDeviceID else { return }
//        Task { @RealmBackgroundActor in
//            try await SyncedDevice.updateLastSeenOnlineIfNeeded(forUUID: currentDeviceID, realmConfiguration: realmConfiguration)
//            try await refreshLastSeenDevices()
//        }
    }
    
    private func syncIsOver() {
//        guard let currentDeviceID = currentDeviceID else { return }
//        Task { @RealmBackgroundActor in
//            try await SyncedDevice.updateLastSeenOnlineIfNeeded(forUUID: currentDeviceID, realmConfiguration: realmConfiguration)
//            try await refreshLastSeenDevices()
//        }
    }
    
    @RealmBackgroundActor
    private func refreshLastSeenDevices() async throws {
        let realm = try await Realm(configuration: realmConfiguration, actor: RealmBackgroundActor.shared)
        let syncedDevices = realm.objects(SyncedDevice.self)
            .where { !$0.isDeleted }
            .sorted(by: \.lastSeenOnline, ascending: false)
        lastSeenDevices = Array(syncedDevices).map {
            LastSeenDevice(
                id: $0.id, deviceName: $0.deviceName,
                lastSeenOnline: $0.lastSeenOnline
            )
        }
    }
}
