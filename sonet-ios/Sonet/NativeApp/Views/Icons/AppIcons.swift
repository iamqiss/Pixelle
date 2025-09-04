import SwiftUI

// Central registry of SF Symbols used across the iOS app.
// Prefer referencing icons via AppIcons to keep usage consistent and enable future theming.
enum AppIcons {
    // Core navigation
    static let home = "house"
    static let video = "video"
    static let messages = "message"
    static let notifications = "bell"
    static let profile = "person"

    // Actions
    static let add = "plus"
    static let search = "magnifyingglass"
    static let share = "square.and.arrow.up"
    static let like = "heart"
    static let likeFilled = "heart.fill"
    static let reply = "message"
    static let repost = "arrow.2.squarepath"
    static let more = "ellipsis"
    static let close = "xmark"
    static let closeCircle = "xmark.circle.fill"
    static let play = "play.fill"
    static let playCircle = "play.circle.fill"
    static let download = "arrow.down.circle"
    static let clock = "clock"
    static let warning = "exclamationmark.triangle"
    static let network = "network"

    // People
    static let person = "person.fill"
    static let personCircle = "person.circle"
    static let verified = "checkmark.seal.fill"
    static let group = "person.3"
    static let shield = "checkmark.shield.fill"

    // Content
    static let photo = "photo"
    static let videoCam = "video"
    static let doc = "doc.fill"

    // Utility
    static let back = "chevron.left"
    static let chevronRight = "chevron.right"
    static let chevronDown = "chevron.down"
    static let arrowUpRight = "arrow.up.right"
    static let location = "location"
    static let link = "link"
    static let calendar = "calendar"
    static let warning = "exclamationmark.triangle"
    static let newspaper = "newspaper"
    static let gear = "gearshape"
    static let location = "location.fill"
    static let link = "link"
    static let calendar = "calendar"
    static let number = "number"
    static let at = "at"
    static let photo = "photo"
    static let shield = "checkmark.shield.fill"
    static let lightbulb = "lightbulb.fill"
    static let docOnDoc = "doc.on.doc"

    // Rendering helper
    @ViewBuilder
    static func image(_ name: String) -> some View {
        Image(systemName: name)
    }
}

struct IconView: View {
    let name: String
    let size: CGFloat
    let weight: Font.Weight
    let color: Color

    init(_ name: String, size: CGFloat = 16, weight: Font.Weight = .regular, color: Color = .primary) {
        self.name = name
        self.size = size
        self.weight = weight
        self.color = color
    }

    var body: some View {
        Image(systemName: name)
            .font(.system(size: size, weight: weight))
            .foregroundColor(color)
    }
}
