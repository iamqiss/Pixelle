import SwiftUI

struct SuggestedSearchCard: View {
    let suggestion: String
    let onTap: () -> Void
    
    @State private var isPressed = false
    
    var body: some View {
        Button(action: onTap) {
            HStack(spacing: 12) {
                // Icon based on suggestion
                IconView(iconForSuggestion, size: 16, color: .accentColor)
                    .frame(width: 24)
                
                // Suggestion text
                Text(suggestion)
                    .font(.system(size: 16, weight: .medium))
                    .foregroundColor(.primary)
                    .lineLimit(1)
                
                Spacer()
                
                // Arrow indicator
                IconView(AppIcons.arrowUpRight, size: 12, color: .secondary)
            }
            .padding(16)
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(Color(.systemGray6))
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
    
    // MARK: - Helper Methods
    private var iconForSuggestion: String {
        switch suggestion.lowercased() {
        case let s where s.contains("tech"):
            return "laptopcomputer"
        case let s where s.contains("science"):
            return "atom"
        case let s where s.contains("art"):
            return "paintbrush"
        case let s where s.contains("music"):
            return "music.note"
        case let s where s.contains("sport"):
            return "sportscourt"
        case let s where s.contains("travel"):
            return "airplane"
        case let s where s.contains("food"):
            return "fork.knife"
        case let s where s.contains("fashion"):
            return "tshirt"
        default:
            return "magnifyingglass"
        }
    }
}

// MARK: - Preview
struct SuggestedSearchCard_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 16) {
            SuggestedSearchCard(suggestion: "Technology") {}
            SuggestedSearchCard(suggestion: "Art") {}
            SuggestedSearchCard(suggestion: "Music") {}
            SuggestedSearchCard(suggestion: "Travel") {}
        }
        .padding()
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}