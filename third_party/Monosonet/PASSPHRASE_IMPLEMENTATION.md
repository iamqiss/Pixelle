# Passphrase Implementation

This document describes the implementation of passphrases to replace traditional passwords in the Sonet authentication system.

## Overview

Traditional passwords have become increasingly problematic:
- Users create weak, guessable passwords
- Complex passwords are hard to remember, leading to forgotten credentials
- Password reuse across multiple accounts creates security vulnerabilities
- Users often resort to writing passwords down, defeating the purpose

Passphrases provide better security through:
- **Easier to remember**: "correct horse battery staple" vs "K9#mP2$vX"
- **Harder to crack**: Longer length means exponentially more possible combinations
- **More natural**: Users can create meaningful, memorable phrases
- **Better entropy**: Even simple words in combination create strong security

## Backend Changes

### Password Manager (`password_manager.cpp`)

The `PasswordManager` class has been updated to support passphrases:

#### New Requirements
- **Minimum length**: 20 characters (increased from 8)
- **Maximum length**: 200 characters (increased from 128)
- **Word count**: Minimum 4 words, maximum 12 words
- **Character requirements**: No longer requires uppercase, digits, or special characters
- **Entropy**: Minimum 8 unique characters

#### New Methods
- `generate_secure_passphrase(size_t word_count = 4)`: Generates random passphrases using common English words
- `is_common_phrase(const std::string& passphrase)`: Checks against forbidden common phrases
- `has_minimum_word_count(const std::string& passphrase)`: Validates word count requirements

#### Forbidden Phrases
Common phrases that are too predictable are now rejected:
- Song lyrics: "twinkle twinkle little star", "mary had a little lamb"
- Nursery rhymes: "baa baa black sheep", "humpty dumpty sat on a wall"
- Tongue twisters: "peter piper picked a peck", "sally sells seashells"
- Famous phrases: "correct horse battery staple", "the quick brown fox"

### Password Policy (`user_types.h`)

Updated `PasswordPolicy` struct with new constants:
```cpp
struct PasswordPolicy {
    static constexpr size_t MIN_LENGTH = 20;        // Minimum 20 characters
    static constexpr size_t MAX_LENGTH = 200;       // Maximum 200 characters
    static constexpr size_t MIN_WORD_COUNT = 4;     // Minimum 4 words
    static constexpr size_t MAX_WORD_COUNT = 12;    // Maximum 12 words
    static constexpr bool REQUIRE_MIXED_CASE = false; // Not required
    static constexpr bool REQUIRE_DIGITS = false;    // Not required
    static constexpr bool REQUIRE_SPECIAL = false;   // Not required
    static constexpr size_t MIN_UNIQUE_CHARS = 8;   // Minimum unique characters
};
```

## Frontend Changes

### Signup Screen

#### Removed Elements
- "You're creating an account on..." starter pack message
- Traditional password requirements (uppercase, digits, special characters)

#### Updated Elements
- **Field label**: Changed from "Password" to "Passphrase"
- **Input label**: Changed from "Choose your password" to "Choose your passphrase"
- **Validation**: Now requires minimum 20 characters and 4 words
- **Help text**: Added guidance explaining passphrases with examples

#### New Help Text
```
A passphrase is 4 or more words that are easy to remember but hard to guess. 
For example: "correct horse battery staple" or "my favorite coffee shop downtown"
```

### Login Screen

#### Updated Elements
- **Field label**: Changed from "Password" to "Passphrase"
- **Accessibility**: Updated hints to reference passphrases
- **Error messages**: Updated to mention passphrases instead of passwords

### Password Reset Flow

#### Updated Elements
- **Form titles**: Changed from "Reset password" to "Reset passphrase"
- **Field labels**: Changed from "New password" to "New passphrase"
- **Help text**: Updated to reference passphrases
- **Success messages**: Updated to mention passphrases

## Validation Logic

### Frontend Validation
```typescript
// Check minimum length
if (password.length < 20) {
  return dispatch({
    type: 'setError',
    value: _('Your passphrase must be at least 20 characters long.'),
    field: 'password',
  })
}

// Check minimum word count
const wordCount = password.trim().split(/\s+/).filter(word => word.length >= 2).length
if (wordCount < 4) {
  return dispatch({
    type: 'setError',
    value: _('Your passphrase must contain at least 4 words.'),
    field: 'password',
  })
}
```

### Passphrase Strength Algorithm

The strength meter uses a sophisticated scoring system:

```typescript
export function calculatePassphraseStrength(passphrase: string): PassphraseStrength {
  if (!passphrase || passphrase.length < 20) return 'weak'
  
  const words = passphrase.trim().split(/\s+/).filter(word => word.length >= 2)
  const uniqueChars = new Set(passphrase.toLowerCase().split('')).size
  const isCommon = isCommonPhrase(passphrase)
  
  let score = 0
  
  // Length score (0-3 points)
  if (passphrase.length >= 20) score += 1
  if (passphrase.length >= 30) score += 1
  if (passphrase.length >= 40) score += 1
  
  // Word count score (0-2 points)
  if (words.length >= 4) score += 1
  if (words.length >= 6) score += 1
  
  // Character variety score (0-2 points)
  if (uniqueChars >= 8) score += 1
  if (uniqueChars >= 12) score += 1
  
  // Bonus for mixed case and numbers (0-1 point)
  if (/[A-Z]/.test(passphrase) && /[a-z]/.test(passphrase)) score += 1
  
  // Penalty for common phrases
  if (isCommon) score = Math.max(0, score - 2)
  
  // Penalty for repeated patterns
  if (/(.+)\1/.test(passphrase)) score = Math.max(0, score - 1)
  
  // Determine strength based on score
  if (score >= 7) return 'excellent'
  if (score >= 5) return 'strong'
  if (score >= 3) return 'good'
  if (score >= 1) return 'fair'
  return 'weak'
}
```

**Strength Levels:**
- **Weak (0 points)**: Too short or too common
- **Fair (1-2 points)**: Meets basic requirements
- **Good (3-4 points)**: Good length and variety
- **Strong (5-6 points)**: Strong and memorable
- **Excellent (7+ points)**: Excellent security and memorability

### Backend Validation
```cpp
bool PasswordManager::is_password_strong(const std::string& passphrase) const {
    // Length check
    if (passphrase.length() < PasswordPolicy::MIN_LENGTH || 
        passphrase.length() > PasswordPolicy::MAX_LENGTH) {
        return false;
    }
    
    // Word count check
    if (!has_minimum_word_count(passphrase)) {
        return false;
    }
    
    // Entropy check
    if (!has_sufficient_entropy(passphrase)) {
        return false;
    }
    
    // Common phrase checks
    if (is_in_common_passwords(passphrase)) return false;
    if (is_common_phrase(passphrase)) return false;
    if (is_keyboard_pattern(passphrase)) return false;
    if (is_repeated_pattern(passphrase)) return false;
    
    return true;
}
```

## Internationalization

All new passphrase-related strings have been added to the English locale file (`en/messages.po`) and are ready for translation to other languages. Key strings include:

- "Passphrase"
- "Choose your passphrase"
- "A passphrase is 4 or more words that are easy to remember but hard to guess..."
- "Your passphrase must be at least 20 characters long"
- "Your passphrase must contain at least 4 words"
- "Reset passphrase"
- "Set new passphrase"
- "Passphrase updated!"

## Security Benefits

1. **Increased Entropy**: 20+ character passphrases provide significantly more entropy than 8-character passwords
2. **Memorability**: Users are more likely to remember meaningful phrases than random character combinations
3. **Reduced Password Reuse**: Easier to remember unique passphrases for different services
4. **Resistance to Dictionary Attacks**: Multiple words in combination are harder to crack than single words
5. **User Adoption**: More user-friendly approach leads to better security practices

## Testing

A comprehensive test suite has been created (`test_passphrase_manager.cpp`) that validates:
- Passphrase strength validation
- Length and word count requirements
- Common phrase rejection
- Secure passphrase generation
- Hashing and verification functionality

## Mobile Layout Optimization

### **Problem Solved**
Traditional password fields treated long text like single-line inputs, causing:
- Text overlapping to the right
- Content getting cut off on mobile devices
- Poor user experience with long passphrases

### **Solution Implemented**
- **Multiline Support**: Passphrase fields now support multiple lines
- **Vertical Expansion**: Fields grow downward as content increases
- **Responsive Height**: Minimum height ensures consistent layout
- **Text Wrapping**: Long passphrases wrap to new lines properly
- **Toggle Button**: Eye icon maintains proper positioning

### **Technical Implementation**
```typescript
// PassphraseInput component with mobile-optimized properties
<TextField.Input
  multiline={true}
  numberOfLines={3}
  textAlignVertical="top"
  style={[
    multiline ? [a.min_h_20, a.py_sm] : [a.min_h_12],
    {textAlignVertical: 'top'},
  ]}
/>
```

### **Mobile Demo Component**
Created `PassphraseMobileDemo.tsx` to showcase:
- Single-line behavior for short passphrases
- Two-line expansion for medium passphrases
- Three+ line expansion for long passphrases
- Proper spacing and no overlap
- Responsive layout on all screen sizes

## Migration Notes

### For Existing Users
- Existing password hashes remain valid
- Users can continue logging in with their current passwords
- Password reset will require new passphrases meeting the new requirements

### For New Users
- All new accounts must use passphrases
- Traditional password requirements no longer apply
- Clear guidance is provided during signup

## Implemented Features

### 1. **Passphrase Strength Meter** ✅
- **Visual Progress Bar**: Color-coded strength indicator (red to green)
- **Strength Levels**: Weak, Fair, Good, Strong, Excellent
- **Real-time Feedback**: Updates as user types
- **Detailed Analysis**: Shows which requirements are met
- **Requirement Checklist**: Visual indicators for each requirement

### 2. **Passphrase Visibility Toggle** ✅
- **Default Visible**: Passphrases are visible by default (no more dots)
- **Toggle Button**: Eye icon to show/hide passphrase
- **Accessibility**: Proper labels and hints for screen readers
- **Multiple Sizes**: Small, medium, and large button variants

### 3. **Enhanced User Experience** ✅
- **Clear Guidance**: Helpful text explaining passphrases
- **Example Passphrases**: "correct horse battery staple" style examples
- **Real-time Validation**: Immediate feedback on strength
- **Visual Requirements**: Checkmarks for met requirements

### 4. **Mobile-Optimized Layout** ✅
- **Vertical Expansion**: Fields expand downward as text grows
- **No Text Overlap**: Long passphrases wrap properly without cutoff
- **Responsive Design**: Adapts to different screen sizes
- **Proper Spacing**: Maintains consistent layout on mobile devices
- **Toggle Button Positioning**: Eye icon stays properly positioned

## Future Enhancements

1. **Passphrase Suggestions**: AI-powered suggestions for memorable but secure passphrases
2. **Multi-language Support**: Passphrase generation in multiple languages
3. **Security Education**: In-app tutorials about passphrase best practices
4. **Breach Monitoring**: Integration with HaveIBeenPwned API for compromised passphrase detection
5. **Strength Meter Animations**: Smooth transitions and micro-interactions

## Conclusion

The implementation of passphrases represents a significant improvement in both security and user experience. By moving away from complex character requirements and focusing on length and memorability, users are more likely to create and maintain secure credentials while reducing the likelihood of forgotten passwords and account lockouts.

This approach aligns with modern security recommendations from organizations like NIST and represents a best practice for authentication systems in 2025 and beyond.