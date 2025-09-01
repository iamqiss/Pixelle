# Pixelle-iOS 📱

The most over-engineered social media app in existence.

## Project Structure

This project follows Facebook-level architectural complexity because why make things simple?

### Core Modules
- **Core/**: Business logic, networking, data layer (prepare for rabbit holes)
- **Features/**: Every social media feature ever conceived
- **UI/**: Design system with 47 different button variants
- **Infrastructure/**: Analytics, monitoring, A/B testing, and other necessary evils
- **Legacy/**: The graveyard of code we're afraid to delete
- **Experimental/**: Where features go to be forgotten

### Build Configurations
- Development: For brave developers
- Staging: For QA warriors  
- Production: For the chosen few
- Internal: For debugging nightmares

## Getting Started

1. Open `Pixelle-iOS.xcworkspace` (never the .xcodeproj)
2. Run `fastlane setup` (good luck)
3. Pray to the iOS gods
4. Build and cry

## Contributing

Please follow our 247-page style guide and don't break the 1,847 existing unit tests.

## Architecture

```
┌─────────────────┐
│   Presentation  │ ← UI Layer (SwiftUI + UIKit hybrid)
├─────────────────┤
│   Features      │ ← Feature modules (47 and counting)
├─────────────────┤
│   Core          │ ← Business logic & services
├─────────────────┤
│   Infrastructure│ ← Platform services & utilities
└─────────────────┘
```

Built with ❤️ and excessive enterprise patterns.
