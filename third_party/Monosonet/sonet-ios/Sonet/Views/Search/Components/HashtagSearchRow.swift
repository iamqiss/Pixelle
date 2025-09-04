import SwiftUI

struct HashtagSearchRow: View {
    let hashtag: String
    @State private var isPressed = false
    
    var body: some View {
        Button(action: {
            // Navigate to hashtag page
        }) {
            HStack(spacing: 12) {
                // Hashtag icon
                IconView(AppIcons.number, size: 16, color: .accentColor)
                    .frame(width: 24)
                
                // Hashtag text
                VStack(alignment: .leading, spacing: 2) {
                    Text("#\(hashtag)")
                        .font(.system(size: 17, weight: .semibold))
                        .foregroundColor(.primary)
                        .lineLimit(1)
                    
                    Text("Explore hashtag")
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                        .lineLimit(1)
                }
                
                Spacer()
                
                // Arrow indicator
                IconView(AppIcons.arrowUpRight, size: 14, color: .secondary)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            .background(
                Rectangle()
                    .fill(Color(.systemBackground))
            )
            .scaleEffect(isPressed ? 0.98 : 1.0)
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
struct HashtagSearchRow_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 0) {
            HashtagSearchRow(hashtag: "Technology")
            HashtagSearchRow(hashtag: "ArtificialIntelligence")
            HashtagSearchRow(hashtag: "Innovation")
        }
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}