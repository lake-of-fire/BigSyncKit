// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "BigSyncKit",
    platforms: [.macOS(.v12), .iOS(.v15), .watchOS(.v4)],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "BigSyncKit",
            type: .dynamic,
            targets: ["BigSyncKit"]),
    ],
    dependencies: [
        //        .package(url: "https://github.com/lake-of-fire/RealmBinary.git", branch: "main"),
        .package(url: "https://github.com/realm/realm-swift.git", from: "10.53.0"),
        .package(url: "https://github.com/lake-of-fire/RealmSwiftGaps.git", branch: "main"),
        //        .package(url: "https://github.com/lake-of-fire/Device.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-async-algorithms", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-algorithms.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-log.git", branch: "main"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "BigSyncKit",
            dependencies: [
//                .product(name: "Realm", package: "RealmBinary"),
//                .product(name: "RealmSwift", package: "RealmBinary"),
                .product(name: "RealmSwift", package: "realm-swift"),
                .product(name: "RealmSwiftGaps", package: "RealmSwiftGaps"),
//                .product(name: "Device Library", package: "Device"),
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                .product(name: "Algorithms", package: "swift-algorithms"),
                .product(name: "Logging", package: "swift-log"),
            ]),
        .testTarget(
            name: "BigSyncKitTests",
            dependencies: ["BigSyncKit"]),
    ]
)
