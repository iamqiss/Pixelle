use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use whatlang::Lang;
use unic_langid::LanguageIdentifier;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageInfo {
    pub code: String,
    pub name: String,
    pub native_name: String,
    pub script: String,
    pub region: Option<String>,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageDetectionResult {
    pub detected_language: LanguageInfo,
    pub confidence: f32,
    pub processing_time_ms: u64,
    pub fallback_used: bool,
}

pub struct LanguageDetector {
    supported_languages: Vec<String>,
    language_cache: Arc<RwLock<HashMap<String, LanguageInfo>>>,
    fallback_language: String,
    min_confidence_threshold: f32,
}

impl LanguageDetector {
    pub fn new(supported_languages: Vec<String>, fallback_language: String) -> Self {
        Self {
            supported_languages,
            language_cache: Arc::new(RwLock::new(HashMap::new())),
            fallback_language,
            min_confidence_threshold: 0.7,
        }
    }

    pub fn is_supported(&self, language_code: &str) -> bool {
        self.supported_languages.contains(&language_code.to_lowercase())
    }

    pub async fn detect(&self, text: &str) -> Result<String> {
        let start_time = std::time::Instant::now();
        
        // Check cache first
        let cache_key = self.generate_cache_key(text);
        if let Some(cached) = self.language_cache.read().await.get(&cache_key) {
            return Ok(cached.code.clone());
        }

        // Use whatlang for fast language detection
        let detection = whatlang::detect(text);
        
        let detected_lang = if let Some(info) = detection {
            let lang_code = info.lang().code().to_string();
            
            if self.is_supported(&lang_code) && info.confidence() > self.min_confidence_threshold {
                lang_code
            } else {
                self.fallback_language.clone()
            }
        } else {
            self.fallback_language.clone()
        };

        let processing_time = start_time.elapsed();
        
        // Cache the result
        let language_info = self.get_language_info(&detected_lang).await?;
        self.language_cache.write().await.insert(cache_key, language_info);

        Ok(detected_lang)
    }

    pub async fn detect_with_details(&self, text: &str) -> Result<LanguageDetectionResult> {
        let start_time = std::time::Instant::now();
        
        let detection = whatlang::detect(text);
        let mut fallback_used = false;
        
        let (detected_lang, confidence) = if let Some(info) = detection {
            let lang_code = info.lang().code().to_string();
            let conf = info.confidence();
            
            if self.is_supported(&lang_code) && conf > self.min_confidence_threshold {
                (lang_code, conf)
            } else {
                fallback_used = true;
                (self.fallback_language.clone(), 0.5)
            }
        } else {
            fallback_used = true;
            (self.fallback_language.clone(), 0.3)
        };

        let processing_time = start_time.elapsed();
        let language_info = self.get_language_info(&detected_lang).await?;

        Ok(LanguageDetectionResult {
            detected_language: language_info,
            confidence,
            processing_time_ms: processing_time.as_millis() as u64,
            fallback_used,
        })
    }

    pub async fn detect_multiple(&self, texts: Vec<String>) -> Result<Vec<String>> {
        let mut results = Vec::new();
        
        for text in texts {
            let detected = self.detect(&text).await?;
            results.push(detected);
        }
        
        Ok(results)
    }

    pub async fn get_language_statistics(&self, texts: Vec<String>) -> Result<HashMap<String, usize>> {
        let mut stats = HashMap::new();
        
        for text in texts {
            let detected = self.detect(&text).await?;
            *stats.entry(detected).or_insert(0) += 1;
        }
        
        Ok(stats)
    }

    async fn get_language_info(&self, language_code: &str) -> Result<LanguageInfo> {
        // Try to parse as LanguageIdentifier first
        if let Ok(lang_id) = LanguageIdentifier::from_str(language_code) {
            let script = lang_id.script().map(|s| s.as_str().to_string()).unwrap_or_default();
            let region = lang_id.region().map(|r| r.as_str().to_string());
            
            return Ok(LanguageInfo {
                code: language_code.to_string(),
                name: self.get_language_name(language_code),
                native_name: self.get_native_name(language_code),
                script,
                region,
                confidence: 1.0,
            });
        }

        // Fallback to basic info
        Ok(LanguageInfo {
            code: language_code.to_string(),
            name: self.get_language_name(language_code),
            native_name: self.get_native_name(language_code),
            script: "".to_string(),
            region: None,
            confidence: 0.8,
        })
    }

    fn get_language_name(&self, code: &str) -> String {
        match code {
            "en" => "English".to_string(),
            "es" => "Spanish".to_string(),
            "fr" => "French".to_string(),
            "de" => "German".to_string(),
            "it" => "Italian".to_string(),
            "pt" => "Portuguese".to_string(),
            "ru" => "Russian".to_string(),
            "zh" => "Chinese".to_string(),
            "ja" => "Japanese".to_string(),
            "ko" => "Korean".to_string(),
            "ar" => "Arabic".to_string(),
            "hi" => "Hindi".to_string(),
            "tr" => "Turkish".to_string(),
            "nl" => "Dutch".to_string(),
            "pl" => "Polish".to_string(),
            "sv" => "Swedish".to_string(),
            "da" => "Danish".to_string(),
            "no" => "Norwegian".to_string(),
            "fi" => "Finnish".to_string(),
            "cs" => "Czech".to_string(),
            "hu" => "Hungarian".to_string(),
            "ro" => "Romanian".to_string(),
            "bg" => "Bulgarian".to_string(),
            "hr" => "Croatian".to_string(),
            "sk" => "Slovak".to_string(),
            "sl" => "Slovenian".to_string(),
            "et" => "Estonian".to_string(),
            "lv" => "Latvian".to_string(),
            "lt" => "Lithuanian".to_string(),
            _ => format!("Unknown ({})", code),
        }
    }

    fn get_native_name(&self, code: &str) -> String {
        match code {
            "en" => "English".to_string(),
            "es" => "Español".to_string(),
            "fr" => "Français".to_string(),
            "de" => "Deutsch".to_string(),
            "it" => "Italiano".to_string(),
            "pt" => "Português".to_string(),
            "ru" => "Русский".to_string(),
            "zh" => "中文".to_string(),
            "ja" => "日本語".to_string(),
            "ko" => "한국어".to_string(),
            "ar" => "العربية".to_string(),
            "hi" => "हिन्दी".to_string(),
            "tr" => "Türkçe".to_string(),
            "nl" => "Nederlands".to_string(),
            "pl" => "Polski".to_string(),
            "sv" => "Svenska".to_string(),
            "da" => "Dansk".to_string(),
            "no" => "Norsk".to_string(),
            "fi" => "Suomi".to_string(),
            "cs" => "Čeština".to_string(),
            "hu" => "Magyar".to_string(),
            "ro" => "Română".to_string(),
            "bg" => "Български".to_string(),
            "hr" => "Hrvatski".to_string(),
            "sk" => "Slovenčina".to_string(),
            "sl" => "Slovenščina".to_string(),
            "et" => "Eesti".to_string(),
            "lv" => "Latviešu".to_string(),
            "lt" => "Lietuvių".to_string(),
            _ => self.get_language_name(code),
        }
    }

    fn generate_cache_key(&self, text: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        format!("lang_{:x}", hasher.finalize())
    }

    pub fn clear_cache(&self) {
        // This would need to be implemented with proper async handling
        // For now, we'll just log the intention
        tracing::info!("Language detection cache clear requested");
    }

    pub fn get_supported_languages(&self) -> Vec<String> {
        self.supported_languages.clone()
    }

    pub fn add_supported_language(&mut self, language_code: String) {
        if !self.supported_languages.contains(&language_code) {
            self.supported_languages.push(language_code);
        }
    }
}

// Language-specific text preprocessing
pub struct LanguagePreprocessor {
    language_detector: Arc<LanguageDetector>,
}

impl LanguagePreprocessor {
    pub fn new(language_detector: Arc<LanguageDetector>) -> Self {
        Self { language_detector }
    }

    pub async fn preprocess_text(&self, text: &str, language_hint: Option<&str>) -> Result<PreprocessedText> {
        let language = if let Some(hint) = language_hint {
            hint.to_string()
        } else {
            self.language_detector.detect(text).await?
        };

        let normalized_text = self.normalize_text(text, &language);
        let tokens = self.tokenize_text(&normalized_text, &language);
        let features = self.extract_features(&tokens, &language);

        Ok(PreprocessedText {
            original: text.to_string(),
            normalized: normalized_text,
            language,
            tokens,
            features,
        })
    }

    fn normalize_text(&self, text: &str, language: &str) -> String {
        let mut normalized = text.to_string();
        
        // Language-specific normalization
        match language {
            "zh" | "ja" | "ko" => {
                // CJK languages: remove extra spaces, normalize characters
                normalized = text.chars()
                    .filter(|&c| !c.is_whitespace() || c == ' ')
                    .collect();
            }
            "ar" | "he" => {
                // RTL languages: normalize diacritics
                normalized = text.chars()
                    .map(|c| self.normalize_rtl_char(c))
                    .collect();
            }
            _ => {
                // Latin-based languages: lowercase, normalize whitespace
                normalized = text.to_lowercase();
                normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
            }
        }
        
        normalized
    }

    fn normalize_rtl_char(&self, c: char) -> char {
        // Basic RTL character normalization
        match c {
            'أ' | 'إ' | 'آ' => 'ا',
            'ؤ' | 'ئ' => 'و',
            'ة' => 'ه',
            _ => c,
        }
    }

    fn tokenize_text(&self, text: &str, language: &str) -> Vec<String> {
        match language {
            "zh" | "ja" | "ko" => {
                // Character-based tokenization for CJK
                text.chars().map(|c| c.to_string()).collect()
            }
            "ar" | "he" => {
                // Word-based tokenization for RTL languages
                text.split_whitespace().map(|s| s.to_string()).collect()
            }
            _ => {
                // Word-based tokenization for Latin-based languages
                text.split_whitespace().map(|s| s.to_string()).collect()
            }
        }
    }

    fn extract_features(&self, tokens: &[String], language: &str) -> HashMap<String, f32> {
        let mut features = HashMap::new();
        
        // Basic features
        features.insert("token_count".to_string(), tokens.len() as f32);
        features.insert("avg_token_length".to_string(), 
            tokens.iter().map(|t| t.len()).sum::<usize>() as f32 / tokens.len() as f32);
        
        // Language-specific features
        match language {
            "en" => {
                // English-specific features
                let has_caps = tokens.iter().any(|t| t.chars().any(|c| c.is_uppercase()));
                features.insert("has_capitalization".to_string(), if has_caps { 1.0 } else { 0.0 });
            }
            "zh" | "ja" | "ko" => {
                // CJK-specific features
                let has_numbers = tokens.iter().any(|t| t.chars().any(|c| c.is_numeric()));
                features.insert("has_numbers".to_string(), if has_numbers { 1.0 } else { 0.0 });
            }
            _ => {}
        }
        
        features
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessedText {
    pub original: String,
    pub normalized: String,
    pub language: String,
    pub tokens: Vec<String>,
    pub features: HashMap<String, f32>,
}

use std::str::FromStr;