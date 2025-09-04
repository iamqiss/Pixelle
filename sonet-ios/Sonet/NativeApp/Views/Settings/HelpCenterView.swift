import SwiftUI

struct HelpCenterView: View {
    @StateObject private var viewModel = HelpCenterViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            List {
                // Quick Help Section
                Section {
                    NavigationLink("Getting Started") {
                        GettingStartedView()
                    }
                    
                    NavigationLink("Account & Profile") {
                        AccountProfileHelpView()
                    }
                    
                    NavigationLink("Privacy & Safety") {
                        PrivacySafetyHelpView()
                    }
                    
                    NavigationLink("Content & Posts") {
                        ContentPostsHelpView()
                    }
                } header: {
                    Text("Quick Help")
                } footer: {
                    Text("Find answers to common questions")
                }
                
                // Search Help Section
                Section {
                    SearchHelpView(
                        searchText: $viewModel.searchText,
                        searchResults: viewModel.searchResults,
                        onSearch: { query in
                            viewModel.searchHelp(query: query)
                        }
                    )
                } header: {
                    Text("Search Help")
                } footer: {
                    Text("Search our help database for specific topics")
                }
                
                // Popular Topics Section
                Section {
                    ForEach(viewModel.popularTopics) { topic in
                        NavigationLink(topic.title) {
                            HelpTopicDetailView(topic: topic)
                        }
                    }
                } header: {
                    Text("Popular Topics")
                } footer: {
                    Text("Most frequently asked questions")
                }
                
                // Contact Support Section
                Section {
                    NavigationLink("Contact Support") {
                        ContactSupportView()
                    }
                    
                    NavigationLink("Report a Problem") {
                        ReportProblemView()
                    }
                    
                    NavigationLink("Request Feature") {
                        RequestFeatureView()
                    }
                    
                    NavigationLink("Bug Report") {
                        BugReportView()
                    }
                } header: {
                    Text("Get Help")
                } footer: {
                    Text("Contact our support team for personalized assistance")
                }
                
                // Help Resources Section
                Section {
                    NavigationLink("Video Tutorials") {
                        VideoTutorialsView()
                    }
                    
                    NavigationLink("User Guide") {
                        UserGuideView()
                    }
                    
                    NavigationLink("Community Forum") {
                        CommunityForumView()
                    }
                    
                    NavigationLink("Developer Documentation") {
                        DeveloperDocsView()
                    }
                } header: {
                    Text("Resources")
                } footer: {
                    Text("Additional learning materials and community support")
                }
                
                // Legal & Terms Section
                Section {
                    NavigationLink("Terms of Service") {
                        TermsOfServiceView()
                    }
                    
                    NavigationLink("Privacy Policy") {
                        PrivacyPolicyView()
                    }
                    
                    NavigationLink("Community Guidelines") {
                        CommunityGuidelinesView()
                    }
                    
                    NavigationLink("Copyright Policy") {
                        CopyrightPolicyView()
                    }
                } header: {
                    Text("Legal & Terms")
                } footer: {
                    Text("Important legal information and community rules")
                }
            }
            .navigationTitle("Help Center")
            .navigationBarTitleDisplayMode(.large)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
        .onAppear {
            viewModel.loadHelpContent()
        }
    }
}

// MARK: - Search Help View
struct SearchHelpView: View {
    @Binding var searchText: String
    let searchResults: [HelpArticle]
    let onSearch: (String) -> Void
    
    var body: some View {
        VStack(spacing: 12) {
            HStack {
                TextField("Search help topics...", text: $searchText)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                
                Button("Search") {
                    onSearch(searchText)
                }
                .buttonStyle(.borderedProminent)
                .disabled(searchText.isEmpty)
            }
            
            if !searchResults.isEmpty {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Search Results")
                        .font(.headline)
                    
                    ForEach(searchResults) { article in
                        NavigationLink(article.title) {
                            HelpArticleDetailView(article: article)
                        }
                        .buttonStyle(PlainButtonStyle())
                        
                        Text(article.excerpt)
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .lineLimit(2)
                    }
                }
            }
        }
    }
}

// MARK: - Help Topic Detail View
struct HelpTopicDetailView: View {
    let topic: HelpTopic
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                // Header
                VStack(alignment: .leading, spacing: 8) {
                    Text(topic.title)
                        .font(.largeTitle)
                        .fontWeight(.bold)
                    
                    Text(topic.description)
                        .font(.body)
                        .foregroundColor(.secondary)
                }
                
                // Content
                VStack(alignment: .leading, spacing: 16) {
                    ForEach(topic.sections) { section in
                        VStack(alignment: .leading, spacing: 8) {
                            Text(section.title)
                                .font(.title2)
                                .fontWeight(.semibold)
                            
                            Text(section.content)
                                .font(.body)
                        }
                    }
                }
                
                // Related Articles
                if !topic.relatedArticles.isEmpty {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Related Articles")
                            .font(.title3)
                            .fontWeight(.semibold)
                        
                        ForEach(topic.relatedArticles) { article in
                            NavigationLink(article.title) {
                                HelpArticleDetailView(article: article)
                            }
                            .buttonStyle(.bordered)
                        }
                    }
                }
                
                // Feedback
                VStack(alignment: .leading, spacing: 12) {
                    Text("Was this helpful?")
                        .font(.headline)
                    
                    HStack(spacing: 16) {
                        Button("Yes") {
                            // Mark as helpful
                        }
                        .buttonStyle(.borderedProminent)
                        
                        Button("No") {
                            // Mark as not helpful
                        }
                        .buttonStyle(.bordered)
                    }
                }
            }
            .padding()
        }
        .navigationTitle(topic.title)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Help Article Detail View
struct HelpArticleDetailView: View {
    let article: HelpArticle
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text(article.title)
                    .font(.largeTitle)
                    .fontWeight(.bold)
                
                Text(article.excerpt)
                    .font(.title3)
                    .foregroundColor(.secondary)
                
                Text(article.content)
                    .font(.body)
                
                if !article.steps.isEmpty {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Steps to Follow")
                            .font(.title2)
                            .fontWeight(.semibold)
                        
                        ForEach(Array(article.steps.enumerated()), id: \.offset) { index, step in
                            HStack(alignment: .top, spacing: 12) {
                                Text("\(index + 1)")
                                    .font(.headline)
                                    .foregroundColor(.white)
                                    .frame(width: 24, height: 24)
                                    .background(Circle().fill(Color.blue))
                                
                                Text(step)
                                    .font(.body)
                            }
                        }
                    }
                }
                
                if !article.tips.isEmpty {
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Pro Tips")
                            .font(.title2)
                            .fontWeight(.semibold)
                        
                        ForEach(article.tips, id: \.self) { tip in
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: "lightbulb.fill")
                                    .foregroundColor(.yellow)
                                
                                Text(tip)
                                    .font(.body)
                            }
                        }
                    }
                }
            }
            .padding()
        }
        .navigationTitle(article.title)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Help Models
struct HelpTopic: Identifiable, Codable {
    let id: String
    let title: String
    let description: String
    let sections: [HelpSection]
    let relatedArticles: [HelpArticle]
    let category: HelpCategory
    let difficulty: HelpDifficulty
}

struct HelpSection: Identifiable, Codable {
    let id: String
    let title: String
    let content: String
}

struct HelpArticle: Identifiable, Codable {
    let id: String
    let title: String
    let excerpt: String
    let content: String
    let steps: [String]
    let tips: [String]
    let category: HelpCategory
    let tags: [String]
}

enum HelpCategory: String, CaseIterable, Codable {
    case gettingStarted = "getting_started"
    case accountProfile = "account_profile"
    case privacySafety = "privacy_safety"
    case contentPosts = "content_posts"
    case features = "features"
    case troubleshooting = "troubleshooting"
    
    var displayName: String {
        switch self {
        case .gettingStarted: return "Getting Started"
        case .accountProfile: return "Account & Profile"
        case .privacySafety: return "Privacy & Safety"
        case .contentPosts: return "Content & Posts"
        case .features: return "Features"
        case .troubleshooting: return "Troubleshooting"
        }
    }
}

enum HelpDifficulty: String, CaseIterable, Codable {
    case beginner = "beginner"
    case intermediate = "intermediate"
    case advanced = "advanced"
    
    var displayName: String {
        switch self {
        case .beginner: return "Beginner"
        case .intermediate: return "Intermediate"
        case .advanced: return "Advanced"
        }
    }
}

// MARK: - Help Center View Model
@MainActor
class HelpCenterViewModel: ObservableObject {
    // MARK: - Published Properties
    @Published var searchText = ""
    @Published var searchResults: [HelpArticle] = []
    @Published var popularTopics: [HelpTopic] = []
    @Published var isLoading = false
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
        loadPopularTopics()
    }
    
    // MARK: - Public Methods
    func loadHelpContent() {
        Task {
            await loadHelpContentFromServer()
        }
    }
    
    func searchHelp(query: String) {
        Task {
            await searchHelpOnServer(query: query)
        }
    }
    
    // MARK: - Private Methods
    private func loadPopularTopics() {
        // Load popular topics from local storage or defaults
        popularTopics = [
            HelpTopic(
                id: "getting-started",
                title: "Getting Started with Sonet",
                description: "Learn the basics of using Sonet",
                sections: [
                    HelpSection(
                        id: "basics",
                        title: "Basic Navigation",
                        content: "Learn how to navigate the app and find your way around."
                    )
                ],
                relatedArticles: [],
                category: .gettingStarted,
                difficulty: .beginner
            ),
            HelpTopic(
                id: "privacy-settings",
                title: "Privacy & Safety Settings",
                description: "Configure your privacy and safety preferences",
                sections: [
                    HelpSection(
                        id: "privacy",
                        title: "Privacy Controls",
                        content: "Control who can see your content and profile information."
                    )
                ],
                relatedArticles: [],
                category: .privacySafety,
                difficulty: .beginner
            )
        ]
    }
    
    private func loadHelpContentFromServer() async {
        isLoading = true
        
        do {
            let request = GetHelpContentRequest.newBuilder()
                .setCategory("all")
                .build()
            
            let response = try await grpcClient.getHelpContent(request: request)
            
            if response.success {
                await MainActor.run {
                    // Update help content from server
                    self.isLoading = false
                }
            } else {
                await MainActor.run {
                    self.isLoading = false
                }
            }
        } catch {
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    private func searchHelpOnServer(query: String) async {
        do {
            let request = SearchHelpRequest.newBuilder()
                .setQuery(query)
                .build()
            
            let response = try await grpcClient.searchHelp(request: request)
            
            if response.success {
                await MainActor.run {
                    self.searchResults = response.articles.map { HelpArticle(from: $0) }
                }
            }
        } catch {
            // Handle error silently for search
            print("Search error: \(error)")
        }
    }
}

// MARK: - Help Article Extension
extension HelpArticle {
    init(from grpcArticle: GRPCHelpArticle) {
        self.id = grpcArticle.id
        self.title = grpcArticle.title
        self.excerpt = grpcArticle.excerpt
        self.content = grpcArticle.content
        self.steps = grpcArticle.steps
        self.tips = grpcArticle.tips
        self.category = HelpCategory(rawValue: grpcArticle.category) ?? .gettingStarted
        self.tags = grpcArticle.tags
    }
}

// MARK: - Placeholder Views (to be implemented)
struct GettingStartedView: View {
    var body: some View {
        Text("Getting Started Guide")
            .navigationTitle("Getting Started")
    }
}

struct AccountProfileHelpView: View {
    var body: some View {
        Text("Account & Profile Help")
            .navigationTitle("Account & Profile")
    }
}

struct PrivacySafetyHelpView: View {
    var body: some View {
        Text("Privacy & Safety Help")
            .navigationTitle("Privacy & Safety")
    }
}

struct ContentPostsHelpView: View {
    var body: some View {
        Text("Content & Posts Help")
            .navigationTitle("Content & Posts")
    }
}

struct ContactSupportView: View {
    var body: some View {
        Text("Contact Support")
            .navigationTitle("Contact Support")
    }
}

struct ReportProblemView: View {
    var body: some View {
        Text("Report a Problem")
            .navigationTitle("Report Problem")
    }
}

struct RequestFeatureView: View {
    var body: some View {
        Text("Request Feature")
            .navigationTitle("Request Feature")
    }
}

struct BugReportView: View {
    var body: some View {
        Text("Bug Report")
            .navigationTitle("Bug Report")
    }
}

struct VideoTutorialsView: View {
    var body: some View {
        Text("Video Tutorials")
            .navigationTitle("Video Tutorials")
    }
}

struct UserGuideView: View {
    var body: some View {
        Text("User Guide")
            .navigationTitle("User Guide")
    }
}

struct CommunityForumView: View {
    var body: some View {
        Text("Community Forum")
            .navigationTitle("Community Forum")
    }
}

struct DeveloperDocsView: View {
    var body: some View {
        Text("Developer Documentation")
            .navigationTitle("Developer Docs")
    }
}

struct TermsOfServiceView: View {
    var body: some View {
        Text("Terms of Service")
            .navigationTitle("Terms of Service")
    }
}

struct PrivacyPolicyView: View {
    var body: some View {
        Text("Privacy Policy")
            .navigationTitle("Privacy Policy")
    }
}

struct CommunityGuidelinesView: View {
    var body: some View {
        Text("Community Guidelines")
            .navigationTitle("Community Guidelines")
    }
}

struct CopyrightPolicyView: View {
    var body: some View {
        Text("Copyright Policy")
            .navigationTitle("Copyright Policy")
    }
}

// MARK: - gRPC Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetHelpContentRequest {
    let category: String
    
    static func newBuilder() -> GetHelpContentRequestBuilder {
        return GetHelpContentRequestBuilder()
    }
}

class GetHelpContentRequestBuilder {
    private var category: String = ""
    
    func setCategory(_ category: String) -> GetHelpContentRequestBuilder {
        self.category = category
        return self
    }
    
    func build() -> GetHelpContentRequest {
        return GetHelpContentRequest(category: category)
    }
}

struct GetHelpContentResponse {
    let success: Bool
    let errorMessage: String
}

struct SearchHelpRequest {
    let query: String
    
    static func newBuilder() -> SearchHelpRequestBuilder {
        return SearchHelpRequestBuilder()
    }
}

class SearchHelpRequestBuilder {
    private var query: String = ""
    
    func setQuery(_ query: String) -> SearchHelpRequestBuilder {
        self.query = query
        return self
    }
    
    func build() -> SearchHelpRequest {
        return SearchHelpRequest(query: query)
    }
}

struct SearchHelpResponse {
    let success: Bool
    let articles: [GRPCHelpArticle]
    let errorMessage: String
}

struct GRPCHelpArticle {
    let id: String
    let title: String
    let excerpt: String
    let content: String
    let steps: [String]
    let tips: [String]
    let category: String
    let tags: [String]
}