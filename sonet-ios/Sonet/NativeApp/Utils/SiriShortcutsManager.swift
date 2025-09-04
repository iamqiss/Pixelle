import Foundation
import Intents
import IntentsUI

@available(iOS 12.0, *)
class SiriShortcutsManager: ObservableObject {
    static let shared = SiriShortcutsManager()
    
    private init() {}
    
    // MARK: - Available Shortcuts
    enum ShortcutType: String, CaseIterable {
        case openDMs = "open_dms"
        case openHome = "open_home"
        case openProfile = "open_profile"
        case composePost = "compose_post"
        case openNotifications = "open_notifications"
        case openVideoFeed = "open_video_feed"
        
        var title: String {
            switch self {
            case .openDMs: return "Open My Sonet DMs"
            case .openHome: return "Open Sonet Home"
            case .openProfile: return "Open My Sonet Profile"
            case .composePost: return "Post on Sonet"
            case .openNotifications: return "Open Sonet Notifications"
            case .openVideoFeed: return "Open Sonet Video Feed"
            }
        }
        
        var suggestedPhrase: String {
            switch self {
            case .openDMs: return "Hey Siri, open my Sonet DMs"
            case .openHome: return "Hey Siri, open Sonet"
            case .openProfile: return "Hey Siri, open my Sonet profile"
            case .composePost: return "Hey Siri, post on Sonet"
            case .openNotifications: return "Hey Siri, open Sonet notifications"
            case .openVideoFeed: return "Hey Siri, open Sonet videos"
            }
        }
    }
    
    // MARK: - Shortcut Creation
    func createShortcut(for type: ShortcutType) -> INShortcut? {
        let intent: INIntent
        
        switch type {
        case .openDMs:
            intent = createOpenDMsIntent()
        case .openHome:
            intent = createOpenHomeIntent()
        case .openProfile:
            intent = createOpenProfileIntent()
        case .composePost:
            intent = createComposePostIntent()
        case .openNotifications:
            intent = createOpenNotificationsIntent()
        case .openVideoFeed:
            intent = createOpenVideoFeedIntent()
        }
        
        return INShortcut(intent: intent)
    }
    
    // MARK: - Intent Creation
    private func createOpenDMsIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://messages")
        return intent
    }
    
    private func createOpenHomeIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://home")
        return intent
    }
    
    private func createOpenProfileIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://profile")
        return intent
    }
    
    private func createComposePostIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://compose")
        return intent
    }
    
    private func createOpenNotificationsIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://notifications")
        return intent
    }
    
    private func createOpenVideoFeedIntent() -> INIntent {
        let intent = INShortcutOpenAppIntent()
        intent.appName = "Sonet"
        intent.url = URL(string: "sonet://video")
        return intent
    }
    
    // MARK: - Shortcut Management
    func addShortcutToSiri(for type: ShortcutType) {
        guard let shortcut = createShortcut(for: type) else { return }
        
        let addVoiceShortcutVC = INUIAddVoiceShortcutViewController(shortcut: shortcut)
        addVoiceShortcutVC.delegate = SiriShortcutDelegate.shared
        
        // Present the view controller (this should be called from a UIViewController)
        if let topVC = UIApplication.shared.topViewController() {
            topVC.present(addVoiceShortcutVC, animated: true)
        }
    }
    
    func suggestShortcuts() {
        let shortcuts = ShortcutType.allCases.compactMap { createShortcut(for: $0) }
        INShortcutCenter.shared.setShortcutSuggestions(shortcuts)
    }
    
    // MARK: - URL Handling
    func handleShortcutURL(_ url: URL) -> Bool {
        guard url.scheme == "sonet" else { return false }
        
        switch url.host {
        case "messages":
            // Navigate to DMs
            NotificationCenter.default.post(name: .navigateToMessages, object: nil)
            return true
        case "home":
            // Navigate to home
            NotificationCenter.default.post(name: .navigateToHome, object: nil)
            return true
        case "profile":
            // Navigate to profile
            NotificationCenter.default.post(name: .navigateToProfile, object: nil)
            return true
        case "compose":
            // Open compose view
            NotificationCenter.default.post(name: .openCompose, object: nil)
            return true
        case "notifications":
            // Navigate to notifications
            NotificationCenter.default.post(name: .navigateToNotifications, object: nil)
            return true
        case "video":
            // Navigate to video feed
            NotificationCenter.default.post(name: .navigateToVideo, object: nil)
            return true
        default:
            return false
        }
    }
}

// MARK: - Siri Shortcut Delegate
@available(iOS 12.0, *)
class SiriShortcutDelegate: NSObject, INUIAddVoiceShortcutViewControllerDelegate {
    static let shared = SiriShortcutDelegate()
    
    private override init() {}
    
    func addVoiceShortcutViewController(_ controller: INUIAddVoiceShortcutViewController, didFinishWith voiceShortcut: INVoiceShortcut?, error: Error?) {
        controller.dismiss(animated: true) {
            if let error = error {
                print("Failed to add Siri shortcut: \(error)")
            } else {
                print("Successfully added Siri shortcut")
            }
        }
    }
    
    func addVoiceShortcutViewControllerDidCancel(_ controller: INUIAddVoiceShortcutViewController) {
        controller.dismiss(animated: true)
    }
}

// MARK: - Notification Names
extension Notification.Name {
    static let navigateToMessages = Notification.Name("navigateToMessages")
    static let navigateToHome = Notification.Name("navigateToHome")
    static let navigateToProfile = Notification.Name("navigateToProfile")
    static let openCompose = Notification.Name("openCompose")
    static let navigateToNotifications = Notification.Name("navigateToNotifications")
    static let navigateToVideo = Notification.Name("navigateToVideo")
}

// MARK: - UIApplication Extension
extension UIApplication {
    func topViewController() -> UIViewController? {
        guard let windowScene = connectedScenes.first as? UIWindowScene,
              let window = windowScene.windows.first else { return nil }
        
        var topController = window.rootViewController
        
        while let presentedController = topController?.presentedViewController {
            topController = presentedController
        }
        
        return topController
    }
}