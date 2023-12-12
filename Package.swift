// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "BigSyncKit",
    platforms: [.macOS(.v12), .iOS(.v15), .watchOS(.v3)],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "BigSyncKit",
            targets: ["BigSyncKit"]),
    ],
    dependencies: [
//        .package(url: "https://github.com/lake-of-fire/RealmBinary.git", branch: "main"),
        .package(url: "https://github.com/realm/realm-swift.git", branch: "master"),
        .package(url: "https://github.com/lake-of-fire/RealmSwiftGaps.git", branch: "main"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "BigSyncKit",
            dependencies: [
//                .product(name: "Realm", package: "RealmBinary"),
//                .product(name: "RealmSwift", package: "RealmBinary"),
                .product(name: "Realm", package: "realm-swift"),
                .product(name: "RealmSwift", package: "realm-swift"),
                .product(name: "RealmSwiftGaps", package: "RealmSwiftGaps"),
            ]),
        .testTarget(
            name: "BigSyncKitTests",
            dependencies: ["BigSyncKit"]),
    ]
)
