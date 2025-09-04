import Foundation
import SwiftUI
import Combine

@MainActor
class NavigationManager: ObservableObject {
    // MARK: - Published Properties
    @Published var selectedTab: Tab = .home
    @Published var navigationPath = NavigationPath()
    @Published var presentedSheet: SheetType?
    @Published var presentedFullScreenCover: FullScreenCoverType?
    
    // MARK: - Private Properties
    private var cancellables = Set<AnyCancellable>()
    private var deepLinkQueue: [DeepLink] = []
    private var isProcessingDeepLinks = false
    
    // MARK: - Initialization
    init() {
        setupNavigationObservers()
    }
    
    // MARK: - Public Methods
    func selectTab(_ tab: Tab) {
        selectedTab = tab
        // Clear navigation stack when switching tabs
        navigationPath = NavigationPath()
    }
    
    func navigateToProfile(username: String) {
        let profileRoute = ProfileRoute.username(username)
        navigationPath.append(profileRoute)
    }
    
    func navigateToNote(noteId: String) {
        let noteRoute = NoteRoute.note(noteId)
        navigationPath.append(noteRoute)
    }
    
    func navigateToSearch(query: String) {
        selectedTab = .search
        let searchRoute = SearchRoute.query(query)
        navigationPath.append(searchRoute)
    }
    
    func presentSheet(_ sheetType: SheetType) {
        presentedSheet = sheetType
    }
    
    func dismissSheet() {
        presentedSheet = nil
    }
    
    func presentFullScreenCover(_ coverType: FullScreenCoverType) {
        presentedFullScreenCover = coverType
    }
    
    func dismissFullScreenCover() {
        presentedFullScreenCover = nil
    }
    
    func handleDeepLink(_ url: URL) {
        let deepLink = parseDeepLink(from: url)
        deepLinkQueue.append(deepLink)
        
        if !isProcessingDeepLinks {
            processDeepLinkQueue()
        }
    }
    
    func goBack() {
        if !navigationPath.isEmpty {
            navigationPath.removeLast()
        }
    }
    
    func goToRoot() {
        navigationPath = NavigationPath()
    }
    
    // MARK: - Private Methods
    private func setupNavigationObservers() {
        // Observe navigation changes for analytics, etc.
        $selectedTab
            .sink { [weak self] tab in
                self?.handleTabChange(to: tab)
            }
            .store(in: &cancellables)
    }
    
    private func handleTabChange(to tab: Tab) {
        // Log tab change for analytics
        print("Tab changed to: \(tab)")
        
        // Handle tab-specific navigation logic
        switch tab {
        case .home:
            // Reset home navigation stack
            break
        case .search:
            // Clear search results when switching to search tab
            break
        case .messages:
            // Handle messages tab specific logic
            break
        case .notifications:
            // Handle notifications tab specific logic
            break
        case .profile:
            // Handle profile tab specific logic
            break
        }
    }
    
    private func parseDeepLink(from url: URL) -> DeepLink {
        // Parse URL components to determine navigation target
        let components = URLComponents(url: url, resolvingAgainstBaseURL: true)
        
        // Example: sonet://profile/username
        if let host = components?.host {
            switch host {
            case "profile":
                if let username = components?.pathComponents.last {
                    return .profile(username: username)
                }
            case "note":
                if let noteId = components?.pathComponents.last {
                    return .note(noteId: noteId)
                }
            case "search":
                if let query = components?.queryItems?.first(where: { $0.name == "q" })?.value {
                    return .search(query: query)
                }
            default:
                break
            }
        }
        
        return .unknown
    }
    
    private func processDeepLinkQueue() {
        guard !deepLinkQueue.isEmpty else {
            isProcessingDeepLinks = false
            return
        }
        
        isProcessingDeepLinks = true
        
        while !deepLinkQueue.isEmpty {
            let deepLink = deepLinkQueue.removeFirst()
            executeDeepLink(deepLink)
        }
        
        isProcessingDeepLinks = false
    }
    
    private func executeDeepLink(_ deepLink: DeepLink) {
        switch deepLink {
        case .profile(let username):
            navigateToProfile(username: username)
        case .note(let noteId):
            navigateToNote(noteId: noteId)
        case .search(let query):
            navigateToSearch(query: query)
        case .unknown:
            // Handle unknown deep links
            break
        }
    }
}

// MARK: - Navigation Types
enum Tab: Int, CaseIterable {
    case home = 0
    case search = 1
    case messages = 2
    case notifications = 3
    case profile = 4
    
    var title: String {
        switch self {
        case .home: return "Home"
        case .search: return "Search"
        case .messages: return "Messages"
        case .notifications: return "Notifications"
        case .profile: return "Profile"
        }
    }
    
    var iconName: String {
        switch self {
        case .home: return "house"
        case .search: return "magnifyingglass"
        case .messages: return "message"
        case .notifications: return "bell"
        case .profile: return "person"
        }
    }
}

// MARK: - Navigation Routes
enum NavigationRoute: Hashable {
    case profile(ProfileRoute)
    case note(NoteRoute)
    case search(SearchRoute)
    case settings
    case composer
}

enum ProfileRoute: Hashable {
    case username(String)
    case followers(String)
    case following(String)
    case lists(String)
}

enum NoteRoute: Hashable {
    case note(String)
    case thread(String)
    case replies(String)
    case likes(String)
}

enum SearchRoute: Hashable {
    case query(String)
    case hashtag(String)
    case user(String)
}

// MARK: - Sheet Types
enum SheetType: Identifiable {
    case composer
    case settings
    case profileEdit
    case mediaPicker
    
    var id: String {
        switch self {
        case .composer: return "composer"
        case .settings: return "settings"
        case .profileEdit: return "profile_edit"
        case .mediaPicker: return "media_picker"
        }
    }
}

// MARK: - Full Screen Cover Types
enum FullScreenCoverType: Identifiable {
    case imageViewer(String)
    case videoPlayer(String)
    case webView(URL)
    
    var id: String {
        switch self {
        case .imageViewer(let url): return "image_viewer_\(url)"
        case .videoPlayer(let url): return "video_player_\(url)"
        case .webView(let url): return "web_view_\(url.absoluteString)"
        }
    }
}

// MARK: - Deep Link Types
enum DeepLink {
    case profile(username: String)
    case note(noteId: String)
    case search(query: String)
    case unknown
}