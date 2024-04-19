import SwiftUI
import CloudKit
import Combine

public struct LastSeenDevice: Identifiable {
    let id: UUID
    let deviceName: String
    let lastSeenOnline: Date
   let humanReadableLastSeenOnline: String
    
    init(id: UUID, deviceName: String, lastSeenOnline: Date) {
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
            return ""
        }
        humanReadableLastSeenOnline = "\(intervalText) ago"
    }
}

public class SyncStatusViewModel: ObservableObject {
    @Published public var syncStatus: String = "Initializing"
//    @Published public var syncStatusWithoutFailure: String = "Initializing"
    @Published public var syncFailed = false
    @Published public var currentDeviceID: UUID?
    @Published public var lastSeenDevices: [SyncedDevice]?

    private var cancellables = Set<AnyCancellable>()
    
    public init() {
        NotificationCenter.default.publisher(for: .SynchronizerWillSynchronize)
            .sink { [weak self] _ in
                self?.syncStatus = "Preparing to Synchronize"
                self?.syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillFetchChanges)
            .sink { [weak self] _ in
                self?.syncStatus = "Fetching Changes"
                self?.syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillUploadChanges)
            .sink { [weak self] _ in
                self?.syncStatus = "Uploading Changes"
                self?.syncFailed = false
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidSynchronize)
            .sink { [weak self] _ in
                self?.syncStatus = "Synchronization Completed"
                self?.syncFailed = false
                self?.syncIsOver()
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidFailToSynchronize)
            .subscribe(on: DispatchQueue.main)
            .sink { [weak self] notification in
                let userInfo = notification.userInfo
                Task { @MainActor [weak self] in
                    guard let self = self else { return }
                    if let error = userInfo?[CloudKitSynchronizer.errorKey] as? Error {
                        if let error = error as? CKError {
                            syncStatus = "Synchronization Failed: \(String(describing: error).prefix(150))"
                        } else {
                            syncStatus = "Synchronization Failed: \(error.localizedDescription)"
                        }
                    } else {
                        syncStatus = "Synchronization Failed: Unknown Error"
                    }
                    syncFailed = true
                    
                    self?.syncIsOver()
                }
            }
            .store(in: &cancellables)
    }
    
    private func syncIsOver() {
        guard let currentDeviceID = currentDeviceID else { return }
        Task { @RealmBackgroundActor in
            try await SyncedDevice.updateLastSeenOnlineIfNeeded(forUUID: currentDeviceID)
            
            let realm = try await Realm(actor: RealmBackgroundActor.shared)
            let syncedDevices = realm.objects(ofType: SyncedDevice.self)
                .where { !$0.isDeleted }
                .sorted(by: \.lastSeenOnline, ascending: false)
            lastSeenDevices = Array(syncedDevices).map {
                LastSeenDevice(
                    deviceName: $0.deviceName,
                    lastSeenOnline: $0.lastSeenOnline
                )
            }
        }
    }
}
