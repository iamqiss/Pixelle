# Pixelle-iOS Architecture Overview

## The Madness Explained

This project structure represents what happens when you let engineers run wild for 15 years.

### Key Principles
1. **Over-engineer everything** - Why use one library when you can use five?
2. **Abstract all the things** - Factories creating builders that inject managers
3. **Feature flags everywhere** - Control every pixel with A/B tests
4. **Legacy code is forever** - That 2019 code? Still running in production
5. **Experimental features** - Half-finished ideas that might ship someday

### Module Dependencies
```
Features → Core → Infrastructure → ThirdParty
    ↓        ↓         ↓
   UI ←──────┴─────────┘
    ↓
  Legacy (somehow everything depends on this)
```

### Current Tech Debt
- 247 TODO comments
- 15 different networking layers
- 3 abandoned UI frameworks
- 1 cursed legacy manager that no one dares touch
- ∞ feature flags

### Performance Metrics
- Build time: ☕☕☕ (3 coffee minimum)
- Lines of code: Too many to count
- Developer sanity: Deprecated in iOS 12

Good luck! 🫡
