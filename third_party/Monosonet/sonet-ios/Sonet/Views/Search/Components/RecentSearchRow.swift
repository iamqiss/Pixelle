import SwiftUI

struct RecentSearchRow: View {
    let search: String
    let onTap: () -> Void
    let onRemove: () -> Void
    
    @State private var isPressed = false
    @State private var showRemoveButton = false
    
    var body: some View {
        HStack(spacing: 12) {
            // Search icon
            IconView(AppIcons.clock)
                .font(.system(size: 16, weight: .medium))
                .foregroundColor(.secondary)
                .frame(width: 24)
            
            // Search text
            Text(search)
                .font(.system(size: 17))
                .foregroundColor(.primary)
                .lineLimit(1)
            
            Spacer()
            
            // Remove button
            if showRemoveButton {
                Button(action: onRemove) {
                    IconView(AppIcons.closeCircle, size: 18, color: .secondary)
                }
                .buttonStyle(PlainButtonStyle())
                .transition(.scale.combined(with: .opacity))
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(
            Rectangle()
                .fill(Color(.systemBackground))
        )
        .scaleEffect(isPressed ? 0.98 : 1.0)
        .animation(.easeInOut(duration: 0.1), value: isPressed)
        .onTapGesture {
            onTap()
        }
        .onLongPressGesture(minimumDuration: 0.5, maximumDistance: .infinity, pressing: { pressing in
            withAnimation(.easeInOut(duration: 0.2)) {
                showRemoveButton = pressing
            }
        }, perform: {})
        .onTapGesture(count: 1, perform: {
            withAnimation(.easeInOut(duration: 0.1)) {
                isPressed = true
            }
            
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                withAnimation(.easeInOut(duration: 0.1)) {
                    isPressed = false
                }
            }
        })
    }
}

// MARK: - Preview
struct RecentSearchRow_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 0) {
            RecentSearchRow(
                search: "Technology",
                onTap: {},
                onRemove: {}
            )
            
            RecentSearchRow(
                search: "Artificial Intelligence",
                onTap: {},
                onRemove: {}
            )
            
            RecentSearchRow(
                search: "Space Exploration",
                onTap: {},
                onRemove: {}
            )
        }
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}