import SwiftUI
import CloudKit
import Combine

public class SyncStatusViewModel: ObservableObject {
    public static let shared = SyncStatusViewModel()
    
    @Published public var syncStatus: String = "Initializing"
    
    private var cancellables = Set<AnyCancellable>()
    
    public init() {
        NotificationCenter.default.publisher(for: .SynchronizerWillSynchronize)
            .sink { [weak self] _ in
                self?.syncStatus = "Preparing to Synchronize"
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillFetchChanges)
            .sink { [weak self] _ in
                self?.syncStatus = "Fetching Changes"
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerWillUploadChanges)
            .sink { [weak self] _ in
                self?.syncStatus = "Uploading Changes"
            }
            .store(in: &cancellables)
        
        NotificationCenter.default.publisher(for: .SynchronizerDidSynchronize)
            .sink { [weak self] _ in
                self?.syncStatus = "Synchronization Completed"
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
                }
            }
            .store(in: &cancellables)
    }
}
