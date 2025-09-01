// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "Pixelle-iOS",
    platforms: [
        .iOS(.v15)
    ],
    products: [
        .library(name: "PixelleCore", targets: ["PixelleCore"]),
        .library(name: "PixelleUI", targets: ["PixelleUI"]),
        .library(name: "PixelleFeatures", targets: ["PixelleFeatures"])
    ],
    dependencies: [
        // Because we need ALL the dependencies
        .package(url: "https://github.com/Alamofire/Alamofire.git", from: "5.8.0"),
        .package(url: "https://github.com/apollographql/apollo-ios.git", from: "1.0.0"),
        .package(url: "https://github.com/SDWebImage/SDWebImage.git", from: "5.18.0"),
        .package(url: "https://github.com/realm/realm-swift.git", from: "10.45.0")
    ],
    targets: [
        .target(name: "PixelleCore", dependencies: ["Alamofire", "Apollo"]),
        .target(name: "PixelleUI", dependencies: ["PixelleCore", "SDWebImage"]),
        .target(name: "PixelleFeatures", dependencies: ["PixelleCore", "PixelleUI"])
    ]
)
