import SwiftUI

struct SonetLogo: View {
    let size: LogoSize
    let color: Color
    
    enum LogoSize {
        case small, medium, large
        
        var fontSize: CGFloat {
            switch self {
            case .small: return 20
            case .medium: return 28
            case .large: return 36
            }
        }
        
        var fontWeight: Font.Weight {
            switch self {
            case .small: return .semibold
            case .medium: return .bold
            case .large: return .heavy
            }
        }
    }
    
    init(size: LogoSize = .medium, color: Color = .primary) {
        self.size = size
        self.color = color
    }
    
    var body: some View {
        Text("Sonet")
            .font(.system(size: size.fontSize, weight: size.fontWeight, design: .rounded))
            .foregroundColor(color)
            .tracking(1.2) // Letter spacing for premium feel
            .shadow(color: color.opacity(0.3), radius: 1, x: 0, y: 1)
    }
}

// MARK: - Preview
struct SonetLogo_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 20) {
            SonetLogo(size: .small)
            SonetLogo(size: .medium)
            SonetLogo(size: .large)
            
            Divider()
            
            SonetLogo(size: .medium, color: .blue)
            SonetLogo(size: .medium, color: .purple)
        }
        .padding()
        .background(Color(.systemBackground))
    }
}