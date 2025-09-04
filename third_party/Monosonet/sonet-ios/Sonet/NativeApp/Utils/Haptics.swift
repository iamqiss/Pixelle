import UIKit
import CoreHaptics

enum Haptics {
    // MARK: - Basic Haptics
    static func success() {
        let generator = UINotificationFeedbackGenerator()
        generator.notificationOccurred(.success)
    }
    
    static func error() {
        let generator = UINotificationFeedbackGenerator()
        generator.notificationOccurred(.error)
    }
    
    static func warning() {
        let generator = UINotificationFeedbackGenerator()
        generator.notificationOccurred(.warning)
    }
    
    static func light() {
        let generator = UIImpactFeedbackGenerator(style: .light)
        generator.impactOccurred()
    }
    
    static func medium() {
        let generator = UIImpactFeedbackGenerator(style: .medium)
        generator.impactOccurred()
    }
    
    static func heavy() {
        let generator = UIImpactFeedbackGenerator(style: .heavy)
        generator.impactOccurred()
    }
    
    static func selection() {
        let generator = UISelectionFeedbackGenerator()
        generator.selectionChanged()
    }
    
    // MARK: - Sonet-Specific Haptics
    static func likePost() {
        // Subtle vibration when liking a post
        light()
    }
    
    static func longPress() {
        // Medium impact for long-pressing media
        medium()
    }
    
    static func dragStart() {
        // Light impact when starting to drag comments
        light()
    }
    
    static func dragEnd() {
        // Medium impact when finishing drag gesture
        medium()
    }
    
    static func commentInteraction() {
        // Light feedback for comment interactions
        light()
    }
    
    static func mediaInteraction() {
        // Medium feedback for media interactions
        medium()
    }
    
    // MARK: - Advanced Haptics (iOS 13+)
    @available(iOS 13.0, *)
    static func customLikeHaptic() {
        guard CHHapticEngine.capabilitiesForHardware().supportsHaptics else {
            likePost()
            return
        }
        
        do {
            let engine = try CHHapticEngine()
            try engine.start()
            
            let intensity = CHHapticEventParameter(parameterID: .hapticIntensity, value: 0.8)
            let sharpness = CHHapticEventParameter(parameterID: .hapticSharpness, value: 0.3)
            
            let event = CHHapticEvent(eventType: .hapticTransient, parameters: [intensity, sharpness], relativeTime: 0)
            let pattern = try CHHapticPattern(events: [event], parameters: [])
            let player = try engine.makePlayer(with: pattern)
            
            try player.start(atTime: 0)
            engine.stop()
        } catch {
            likePost()
        }
    }
}

