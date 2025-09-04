import SwiftUI

struct TrendingTopicCard: View {
    let topic: SearchViewModel.TrendingTopic
    @State private var isPressed = false
    
    var body: some View {
        Button(action: {
            // Navigate to hashtag page
        }) {
            VStack(alignment: .leading, spacing: 12) {
                // Header
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(topic.hashtag)
                            .font(.system(size: 16, weight: .bold))
                            .foregroundColor(.primary)
                            .lineLimit(1)
                        
                        if let category = topic.category {
                            Text(category)
                                .font(.system(size: 12, weight: .medium))
                                .foregroundColor(.secondary)
                                .lineLimit(1)
                        }
                    }
                    
                    Spacer()
                    
                    // Trending indicator
                    if topic.isTrending {
                        HStack(spacing: 4) {
                            IconView("flame.fill", size: 10, color: .accentColor)
                            
                            Text("Trending")
                                .font(.system(size: 10, weight: .bold))
                                .foregroundColor(.accentColor)
                        }
                        .padding(.horizontal, 8)
                        .padding(.vertical, 4)
                        .background(
                            RoundedRectangle(cornerRadius: 12)
                                .fill(Color.accentColor.opacity(0.1))
                        )
                    }
                }
                
                // Post count
                HStack {
                    Text("\(topic.postCount.formatted()) posts")
                        .font(.system(size: 14, weight: .medium))
                        .foregroundColor(.secondary)
                    
                    Spacer()
                    
                    // Arrow indicator
                    IconView(AppIcons.arrowUpRight, size: 12, color: .accentColor)
                }
            }
            .padding(16)
            .frame(width: 200)
            .background(
                RoundedRectangle(cornerRadius: 16)
                    .fill(Color(.systemGray6))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16)
                            .stroke(
                                topic.isTrending ? Color.accentColor.opacity(0.3) : Color.clear,
                                lineWidth: 1
                            )
                    )
            )
            .scaleEffect(isPressed ? 0.95 : 1.0)
            .animation(.easeInOut(duration: 0.1), value: isPressed)
        }
        .buttonStyle(PlainButtonStyle())
        .onLongPressGesture(minimumDuration: 0, maximumDistance: .infinity, pressing: { pressing in
            withAnimation(.easeInOut(duration: 0.1)) {
                isPressed = pressing
            }
        }, perform: {})
    }
}

// MARK: - Preview
struct TrendingTopicCard_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 20) {
            TrendingTopicCard(
                topic: SearchViewModel.TrendingTopic(
                    hashtag: "#TechNews",
                    postCount: 15420,
                    isTrending: true,
                    category: "Technology"
                )
            )
            
            TrendingTopicCard(
                topic: SearchViewModel.TrendingTopic(
                    hashtag: "#ClimateAction",
                    postCount: 9870,
                    isTrending: false,
                    category: "Environment"
                )
            )
        }
        .padding()
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}