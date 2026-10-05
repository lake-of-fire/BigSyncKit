// swift-tools-version: 6.0
import PackageDescription
let package = Package(name: "DisappearanceReadBoundary", targets: [
    .target(name: "CloudKit"),
    .target(name: "RealmSwift"),
    .target(name: "RealmSwiftGaps"),
    .target(name: "BigSyncKit", dependencies: ["CloudKit", "RealmSwift", "RealmSwiftGaps"]),
    .testTarget(name: "BoundaryTests", dependencies: ["BigSyncKit", "CloudKit", "RealmSwift"])
])
