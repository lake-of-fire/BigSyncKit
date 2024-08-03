import Foundation

@globalActor
public actor BigSyncBackgroundActor {
    public static var shared = BigSyncBackgroundActor()
    
    public init() { }
}
