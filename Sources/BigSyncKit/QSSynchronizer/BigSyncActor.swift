import Foundation

@globalActor
public actor BigSyncActor {
    public static var shared = BigSyncActor()
    
    public init() { }
}
