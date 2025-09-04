import ActivityKit
import SwiftUI

struct UploadActivityAttributes: ActivityAttributes {
    public struct ContentState: Codable, Hashable {
        var progress: Double
        var filename: String
        var isCompleted: Bool
    }

    var title: String
}

@available(iOS 16.1, *)
enum UploadActivityManager {
    static func start(title: String, filename: String) -> Activity<UploadActivityAttributes>? {
        guard ActivityAuthorizationInfo().areActivitiesEnabled else { return nil }
        let attr = UploadActivityAttributes(title: title)
        let state = UploadActivityAttributes.ContentState(progress: 0.0, filename: filename, isCompleted: false)
        do {
            return try Activity.request(attributes: attr, contentState: state, pushType: nil)
        } catch {
            return nil
        }
    }

    static func update(_ activity: Activity<UploadActivityAttributes>, progress: Double, filename: String, isCompleted: Bool) {
        let state = UploadActivityAttributes.ContentState(progress: progress, filename: filename, isCompleted: isCompleted)
        Task { await activity.update(using: state) }
        if isCompleted { Task { await activity.end(dismissalPolicy: .immediate) } }
    }
}

