use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::core::languages::LanguageDetector;
use crate::core::ml_models::MlModelManager;
use crate::core::signals::SignalProcessor;
use crate::core::observability::MetricsCollector;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ClassificationLabel {
    // Content Safety
    Spam,
    HateSpeech,
    Harassment,
    Violence,
    Csam,
    Misinformation,
    Copyright,
    
    // Content Quality
    LowQuality,
    Duplicate,
    OffTopic,
    
    // User Behavior
    Bot,
    Troll,
    Impersonation,
    
    // Safe Content
    Clean,
    Educational,
    Creative,
    
    // Custom labels for specific domains
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    pub id: Uuid,
    pub content_id: String,
    pub user_id: String,
    pub label: ClassificationLabel,
    pub confidence: f32,
    pub language: String,
    pub detected_language: Option<String>,
    pub processing_time_ms: u64,
    pub model_version: String,
    pub signals: Vec<String>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationRequest {
    pub content_id: String,
    pub user_id: String,
    pub text: String,
    pub content_type: ContentType,
    pub language_hint: Option<String>,
    pub context: HashMap<String, serde_json::Value>,
    pub priority: Priority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContentType {
    Post,
    Comment,
    Message,
    Profile,
    Media,
    Link,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierConfig {
    pub min_confidence_threshold: f32,
    pub max_processing_time_ms: u64,
    pub enable_language_detection: bool,
    pub enable_ml_inference: bool,
    pub enable_rule_based: bool,
    pub enable_signal_processing: bool,
    pub batch_processing: bool,
    pub cache_results: bool,
}

pub trait ContentClassifier: Send + Sync {
    async fn classify(&self, request: ClassificationRequest) -> Result<ClassificationResult>;
    async fn classify_batch(&self, requests: Vec<ClassificationRequest>) -> Result<Vec<ClassificationResult>>;
    async fn get_model_info(&self) -> Result<ModelInfo>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub name: String,
    pub version: String,
    pub supported_languages: Vec<String>,
    pub last_updated: DateTime<Utc>,
    pub performance_metrics: HashMap<String, f64>,
}

pub struct ProductionClassifier {
    config: ClassifierConfig,
    language_detector: Arc<LanguageDetector>,
    ml_models: Arc<MlModelManager>,
    signal_processor: Arc<SignalProcessor>,
    metrics: Arc<MetricsCollector>,
    cache: Arc<RwLock<HashMap<String, ClassificationResult>>>,
    rule_engine: Arc<RuleBasedClassifier>,
}

impl ProductionClassifier {
    pub fn new(
        config: ClassifierConfig,
        language_detector: Arc<LanguageDetector>,
        ml_models: Arc<MlModelManager>,
        signal_processor: Arc<SignalProcessor>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            config,
            language_detector,
            ml_models,
            signal_processor,
            metrics,
            cache: Arc::new(RwLock::new(HashMap::new())),
            rule_engine: Arc::new(RuleBasedClassifier::new()),
        }
    }

    async fn detect_language(&self, text: &str, hint: Option<&str>) -> Result<String> {
        if let Some(hint) = hint {
            if self.language_detector.is_supported(hint) {
                return Ok(hint.to_string());
            }
        }
        
        self.language_detector.detect(text).await
    }

    async fn process_with_ml(&self, text: &str, language: &str) -> Result<(ClassificationLabel, f32)> {
        if !self.config.enable_ml_inference {
            return Ok((ClassificationLabel::Clean, 0.5));
        }

        let start_time = std::time::Instant::now();
        let result = self.ml_models.classify_text(text, language).await?;
        let processing_time = start_time.elapsed();

        self.metrics.record_ml_inference_time(processing_time.as_millis() as u64);
        
        Ok(result)
    }

    async fn process_signals(&self, request: &ClassificationRequest, result: &ClassificationResult) -> Result<Vec<String>> {
        if !self.config.enable_signal_processing {
            return Ok(vec![]);
        }

        self.signal_processor.process_content_signals(request, result).await
    }
}

impl ContentClassifier for ProductionClassifier {
    async fn classify(&self, request: ClassificationRequest) -> Result<ClassificationResult> {
        let start_time = std::time::Instant::now();
        
        // Check cache first
        if self.config.cache_results {
            if let Some(cached) = self.cache.read().await.get(&request.content_id) {
                self.metrics.record_cache_hit();
                return Ok(cached.clone());
            }
        }

        // Language detection
        let language = self.detect_language(&request.text, request.language_hint.as_deref()).await?;
        let detected_language = if request.language_hint.is_none() {
            Some(language.clone())
        } else {
            None
        };

        // ML-based classification
        let (ml_label, ml_confidence) = self.process_with_ml(&request.text, &language).await?;

        // Rule-based classification
        let rule_result = if self.config.enable_rule_based {
            self.rule_engine.classify(&request.text)
        } else {
            ClassificationResult {
                id: Uuid::new_v4(),
                content_id: request.content_id.clone(),
                user_id: request.user_id.clone(),
                label: ClassificationLabel::Clean,
                confidence: 0.5,
                language: language.clone(),
                detected_language: detected_language.clone(),
                processing_time_ms: 0,
                model_version: "rule-based".to_string(),
                signals: vec![],
                metadata: HashMap::new(),
                timestamp: Utc::now(),
            }
        };

        // Combine results (ML takes precedence if confidence is high enough)
        let (final_label, final_confidence) = if ml_confidence > self.config.min_confidence_threshold {
            (ml_label, ml_confidence)
        } else {
            (rule_result.label, rule_result.confidence)
        };

        let processing_time = start_time.elapsed();
        let processing_time_ms = processing_time.as_millis() as u64;

        // Process signals
        let signals = self.process_signals(&request, &ClassificationResult {
            id: Uuid::new_v4(),
            content_id: request.content_id.clone(),
            user_id: request.user_id.clone(),
            label: final_label.clone(),
            confidence: final_confidence,
            language: language.clone(),
            detected_language: detected_language.clone(),
            processing_time_ms,
            model_version: "production".to_string(),
            signals: vec![],
            metadata: HashMap::new(),
            timestamp: Utc::now(),
        }).await?;

        let result = ClassificationResult {
            id: Uuid::new_v4(),
            content_id: request.content_id,
            user_id: request.user_id,
            label: final_label,
            confidence: final_confidence,
            language,
            detected_language,
            processing_time_ms,
            model_version: self.ml_models.get_model_version().await?,
            signals,
            metadata: request.context,
            timestamp: Utc::now(),
        };

        // Cache result
        if self.config.cache_results {
            self.cache.write().await.insert(result.content_id.clone(), result.clone());
        }

        // Record metrics
        self.metrics.record_classification_time(processing_time_ms);
        self.metrics.record_classification_result(&result);

        Ok(result)
    }

    async fn classify_batch(&self, requests: Vec<ClassificationRequest>) -> Result<Vec<ClassificationResult>> {
        if !self.config.batch_processing {
            let mut results = Vec::new();
            for request in requests {
                results.push(self.classify(request).await?);
            }
            return Ok(results);
        }

        // Implement batch processing for better throughput
        let mut results = Vec::new();
        let batch_size = 32; // Configurable batch size
        
        for chunk in requests.chunks(batch_size) {
            let futures: Vec<_> = chunk.iter().map(|req| self.classify(req.clone())).collect();
            let chunk_results = futures::future::join_all(futures).await;
            
            for result in chunk_results {
                results.push(result?);
            }
        }

        Ok(results)
    }

    async fn get_model_info(&self) -> Result<ModelInfo> {
        self.ml_models.get_model_info().await
    }
}

// Enhanced rule-based classifier
pub struct RuleBasedClassifier {
    patterns: HashMap<ClassificationLabel, Vec<String>>,
    regex_patterns: HashMap<ClassificationLabel, Vec<regex::Regex>>,
}

impl RuleBasedClassifier {
    pub fn new() -> Self {
        let mut patterns = HashMap::new();
        let mut regex_patterns = HashMap::new();

        // Spam patterns
        patterns.insert(ClassificationLabel::Spam, vec![
            "buy now", "free $$$", "click here", "limited offer", "act now",
            "make money fast", "work from home", "earn $1000", "guaranteed results"
        ]);

        // Hate speech patterns
        patterns.insert(ClassificationLabel::HateSpeech, vec![
            "hate", "racist", "bigot", "supremacist", "nazi"
        ]);

        // Violence patterns
        patterns.insert(ClassificationLabel::Violence, vec![
            "kill", "murder", "attack", "bomb", "shoot"
        ]);

        // Build regex patterns
        for (label, pattern_list) in &patterns {
            let regex_list: Vec<regex::Regex> = pattern_list
                .iter()
                .map(|p| regex::Regex::new(&format!(r"(?i){}", p)).unwrap())
                .collect();
            regex_patterns.insert(label.clone(), regex_list);
        }

        Self {
            patterns,
            regex_patterns,
        }
    }

    pub fn classify(&self, text: &str) -> ClassificationResult {
        let mut best_label = ClassificationLabel::Clean;
        let mut best_confidence = 0.0;

        for (label, regex_list) in &self.regex_patterns {
            let mut matches = 0;
            for regex in regex_list {
                if regex.is_match(text) {
                    matches += 1;
                }
            }

            if matches > 0 {
                let confidence = (matches as f32 / regex_list.len() as f32).min(0.9);
                if confidence > best_confidence {
                    best_confidence = confidence;
                    best_label = label.clone();
                }
            }
        }

        ClassificationResult {
            id: Uuid::new_v4(),
            content_id: "".to_string(),
            user_id: "".to_string(),
            label: best_label,
            confidence: best_confidence.max(0.1),
            language: "en".to_string(),
            detected_language: None,
            processing_time_ms: 0,
            model_version: "rule-based".to_string(),
            signals: vec![],
            metadata: HashMap::new(),
            timestamp: Utc::now(),
        }
    }
}