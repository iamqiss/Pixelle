use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::path::Path;
use tokio::sync::{RwLock, Semaphore};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::core::classifier::{ClassificationLabel, ClassificationResult};
use crate::core::languages::PreprocessedText;
use crate::core::observability::MetricsCollector;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub supported_languages: Vec<String>,
    pub model_type: ModelType,
    pub file_size: u64,
    pub checksum: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub performance_metrics: HashMap<String, f64>,
    pub hyperparameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelType {
    Bert,
    Distilbert,
    Roberta,
    XlmRoberta,
    FastText,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPerformance {
    pub model_id: String,
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
    pub latency_ms: f64,
    pub throughput: f64,
    pub last_evaluated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub id: Uuid,
    pub text: String,
    pub language: String,
    pub content_type: String,
    pub priority: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    pub request_id: Uuid,
    pub label: ClassificationLabel,
    pub confidence: f32,
    pub processing_time_ms: u64,
    pub model_version: String,
    pub features: HashMap<String, f32>,
    pub metadata: HashMap<String, serde_json::Value>,
}

pub struct MlModelManager {
    models: Arc<RwLock<HashMap<String, Arc<ModelInstance>>>>,
    active_model: Arc<RwLock<String>>,
    model_cache: Arc<RwLock<HashMap<String, ModelCacheEntry>>>,
    inference_semaphore: Arc<Semaphore>,
    metrics: Arc<MetricsCollector>,
    config: ModelManagerConfig,
}

#[derive(Debug, Clone)]
pub struct ModelManagerConfig {
    pub max_concurrent_inferences: usize,
    pub model_cache_size: usize,
    pub batch_size: usize,
    pub gpu_enabled: bool,
    pub model_path: String,
    pub enable_model_auto_update: bool,
    pub model_health_check_interval: std::time::Duration,
}

impl Default for ModelManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_inferences: 16,
            model_cache_size: 1000,
            batch_size: 32,
            gpu_enabled: false,
            model_path: "./models".to_string(),
            enable_model_auto_update: true,
            model_health_check_interval: std::time::Duration::from_secs(300), // 5 minutes
        }
    }
}

pub struct ModelInstance {
    metadata: ModelMetadata,
    model: Box<dyn InferenceModel>,
    tokenizer: Option<Box<dyn Tokenizer>>,
    preprocessor: Box<dyn TextPreprocessor>,
}

pub struct ModelCacheEntry {
    pub result: InferenceResult,
    pub created_at: DateTime<Utc>,
    pub access_count: u64,
}

pub trait InferenceModel: Send + Sync {
    async fn predict(&self, input: &str, language: &str) -> Result<(ClassificationLabel, f32)>;
    async fn predict_batch(&self, inputs: &[String], languages: &[String]) -> Result<Vec<(ClassificationLabel, f32)>>;
    async fn get_model_info(&self) -> Result<ModelMetadata>;
    async fn is_healthy(&self) -> bool;
}

pub trait Tokenizer: Send + Sync {
    async fn tokenize(&self, text: &str) -> Result<Vec<String>>;
    async fn encode(&self, text: &str) -> Result<Vec<u32>>;
    async fn decode(&self, tokens: &[u32]) -> Result<String>;
}

pub trait TextPreprocessor: Send + Sync {
    async fn preprocess(&self, text: &str, language: &str) -> Result<PreprocessedText>;
    async fn extract_features(&self, text: &str, language: &str) -> Result<HashMap<String, f32>>;
}

impl MlModelManager {
    pub fn new(config: ModelManagerConfig, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            models: Arc::new(RwLock::new(HashMap::new())),
            active_model: Arc::new(RwLock::new("default".to_string())),
            model_cache: Arc::new(RwLock::new(HashMap::new())),
            inference_semaphore: Arc::new(Semaphore::new(config.max_concurrent_inferences)),
            metrics,
            config,
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        tracing::info!("Initializing ML Model Manager");
        
        // Load available models
        self.load_available_models().await?;
        
        // Set default active model
        if let Some(first_model) = self.models.read().await.keys().next().cloned() {
            *self.active_model.write().await = first_model;
        }
        
        // Start health check loop
        self.start_health_check_loop();
        
        tracing::info!("ML Model Manager initialized successfully");
        Ok(())
    }

    pub async fn load_available_models(&self) -> Result<()> {
        let model_path = Path::new(&self.config.model_path);
        if !model_path.exists() {
            tracing::warn!("Model path does not exist: {:?}", model_path);
            return Ok(());
        }

        for entry in std::fs::read_dir(model_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(model_id) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(model) = self.load_model(model_id, &path).await {
                        self.models.write().await.insert(model_id.to_string(), Arc::new(model));
                        tracing::info!("Loaded model: {}", model_id);
                    }
                }
            }
        }

        Ok(())
    }

    async fn load_model(&self, model_id: &str, path: &Path) -> Result<ModelInstance> {
        // This is a simplified model loading implementation
        // In production, you would load actual ML models (BERT, etc.)
        
        let metadata = ModelMetadata {
            id: model_id.to_string(),
            name: model_id.to_string(),
            version: "1.0.0".to_string(),
            description: format!("Model loaded from {:?}", path),
            supported_languages: vec!["en".to_string(), "es".to_string(), "fr".to_string()],
            model_type: ModelType::Bert,
            file_size: 0,
            checksum: "".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            performance_metrics: HashMap::new(),
            hyperparameters: HashMap::new(),
        };

        let model = Box::new(DummyInferenceModel::new(metadata.clone()));
        let preprocessor = Box::new(DefaultTextPreprocessor::new());

        Ok(ModelInstance {
            metadata,
            model,
            tokenizer: None,
            preprocessor,
        })
    }

    pub async fn classify_text(&self, text: &str, language: &str) -> Result<(ClassificationLabel, f32)> {
        let start_time = std::time::Instant::now();
        
        // Check cache first
        let cache_key = self.generate_cache_key(text, language);
        if let Some(cached) = self.model_cache.read().await.get(&cache_key) {
            self.metrics.record_cache_hit();
            return Ok((cached.result.label.clone(), cached.result.confidence));
        }

        // Acquire semaphore for concurrent inference control
        let _permit = self.inference_semaphore.acquire().await?;
        
        // Get active model
        let active_model_id = self.active_model.read().await.clone();
        let model = self.models.read().await.get(&active_model_id)
            .ok_or_else(|| anyhow::anyhow!("Active model not found: {}", active_model_id))?
            .clone();

        // Perform inference
        let result = model.model.predict(text, language).await?;
        let processing_time = start_time.elapsed();

        // Cache result
        let inference_result = InferenceResult {
            request_id: Uuid::new_v4(),
            label: result.0.clone(),
            confidence: result.1,
            processing_time_ms: processing_time.as_millis() as u64,
            model_version: model.metadata.version.clone(),
            features: HashMap::new(),
            metadata: HashMap::new(),
        };

        self.cache_result(cache_key, inference_result).await;

        // Record metrics
        self.metrics.record_inference_time(processing_time.as_millis() as u64);
        self.metrics.record_inference_result(&result.0, result.1);

        Ok(result)
    }

    pub async fn classify_batch(&self, texts: &[String], languages: &[String]) -> Result<Vec<(ClassificationLabel, f32)>> {
        if texts.len() != languages.len() {
            return Err(anyhow::anyhow!("Texts and languages arrays must have the same length"));
        }

        let start_time = std::time::Instant::now();
        
        // Get active model
        let active_model_id = self.active_model.read().await.clone();
        let model = self.models.read().await.get(&active_model_id)
            .ok_or_else(|| anyhow::anyhow!("Active model not found: {}", active_model_id))?
            .clone();

        // Perform batch inference
        let results = model.model.predict_batch(texts, languages).await?;
        let processing_time = start_time.elapsed();

        // Record batch metrics
        self.metrics.record_batch_inference_time(processing_time.as_millis() as u64, texts.len());
        self.metrics.record_batch_inference_results(&results);

        Ok(results)
    }

    pub async fn get_model_info(&self) -> Result<ModelMetadata> {
        let active_model_id = self.active_model.read().await.clone();
        let model = self.models.read().await.get(&active_model_id)
            .ok_or_else(|| anyhow::anyhow!("Active model not found: {}", active_model_id))?;
        
        Ok(model.metadata.clone())
    }

    pub async fn get_model_version(&self) -> Result<String> {
        let info = self.get_model_info().await?;
        Ok(info.version)
    }

    pub async fn switch_model(&self, model_id: &str) -> Result<()> {
        if !self.models.read().await.contains_key(model_id) {
            return Err(anyhow::anyhow!("Model not found: {}", model_id));
        }

        *self.active_model.write().await = model_id.to_string();
        tracing::info!("Switched to model: {}", model_id);
        
        Ok(())
    }

    pub async fn get_available_models(&self) -> Vec<ModelMetadata> {
        self.models.read().await.values()
            .map(|m| m.metadata.clone())
            .collect()
    }

    pub async fn get_model_performance(&self, model_id: &str) -> Result<ModelPerformance> {
        // This would typically query a metrics database
        // For now, return dummy data
        Ok(ModelPerformance {
            model_id: model_id.to_string(),
            accuracy: 0.95,
            precision: 0.94,
            recall: 0.93,
            f1_score: 0.935,
            latency_ms: 50.0,
            throughput: 1000.0,
            last_evaluated: Utc::now(),
        })
    }

    async fn cache_result(&self, key: String, result: InferenceResult) {
        let mut cache = self.model_cache.write().await;
        
        // Implement LRU eviction if cache is full
        if cache.len() >= self.config.model_cache_size {
            let oldest_key = cache.iter()
                .min_by_key(|(_, entry)| entry.created_at)
                .map(|(k, _)| k.clone());
            
            if let Some(key_to_remove) = oldest_key {
                cache.remove(&key_to_remove);
            }
        }
        
        cache.insert(key, ModelCacheEntry {
            result,
            created_at: Utc::now(),
            access_count: 1,
        });
    }

    fn generate_cache_key(&self, text: &str, language: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        hasher.update(language.as_bytes());
        format!("ml_{:x}", hasher.finalize())
    }

    fn start_health_check_loop(&self) {
        let models = self.models.clone();
        let active_model = self.active_model.clone();
        let interval = self.config.model_health_check_interval;
        
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // Check health of all models
                let models_guard = models.read().await;
                let mut active_model_guard = active_model.write().await;
                
                for (model_id, model) in models_guard.iter() {
                    if !model.model.is_healthy().await {
                        tracing::warn!("Model {} is unhealthy", model_id);
                        
                        // Switch to a healthy model if current one is unhealthy
                        if *active_model_guard == *model_id {
                            for (healthy_id, healthy_model) in models_guard.iter() {
                                if healthy_model.model.is_healthy().await {
                                    *active_model_guard = healthy_id.clone();
                                    tracing::info!("Switched to healthy model: {}", healthy_id);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

// Dummy implementation for testing
struct DummyInferenceModel {
    metadata: ModelMetadata,
}

impl DummyInferenceModel {
    fn new(metadata: ModelMetadata) -> Self {
        Self { metadata }
    }
}

impl InferenceModel for DummyInferenceModel {
    async fn predict(&self, _input: &str, _language: &str) -> Result<(ClassificationLabel, f32)> {
        // Simulate ML inference
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        
        // Return dummy classification
        let labels = vec![
            ClassificationLabel::Clean,
            ClassificationLabel::Spam,
            ClassificationLabel::HateSpeech,
        ];
        
        let random_label = labels[rand::random::<usize>() % labels.len()].clone();
        let confidence = 0.7 + (rand::random::<f32>() * 0.3);
        
        Ok((random_label, confidence))
    }

    async fn predict_batch(&self, inputs: &[String], _languages: &[String]) -> Result<Vec<(ClassificationLabel, f32)>> {
        let mut results = Vec::new();
        
        for _input in inputs {
            let result = self.predict(_input, "en").await?;
            results.push(result);
        }
        
        Ok(results)
    }

    async fn get_model_info(&self) -> Result<ModelMetadata> {
        Ok(self.metadata.clone())
    }

    async fn is_healthy(&self) -> bool {
        true
    }
}

struct DefaultTextPreprocessor;

impl DefaultTextPreprocessor {
    fn new() -> Self {
        Self
    }
}

impl TextPreprocessor for DefaultTextPreprocessor {
    async fn preprocess(&self, text: &str, _language: &str) -> Result<PreprocessedText> {
        let normalized = text.to_lowercase();
        let tokens = normalized.split_whitespace().map(|s| s.to_string()).collect();
        let features = HashMap::new();
        
        Ok(PreprocessedText {
            original: text.to_string(),
            normalized,
            language: "en".to_string(),
            tokens,
            features,
        })
    }

    async fn extract_features(&self, _text: &str, _language: &str) -> Result<HashMap<String, f32>> {
        Ok(HashMap::new())
    }
}

// Add rand dependency for dummy model
use rand;