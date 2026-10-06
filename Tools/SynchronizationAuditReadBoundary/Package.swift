// swift-tools-version: 6.0
import PackageDescription
let package = Package(name: "SynchronizationAuditReadBoundary", targets: [
    .target(name: "CloudKit"),
    .target(name: "RealmSwift"),
    .target(name: "BigSyncKit", dependencies: ["CloudKit", "RealmSwift"]),
    .testTarget(name: "BoundaryTests", dependencies: ["BigSyncKit", "CloudKit", "RealmSwift"]),
])
