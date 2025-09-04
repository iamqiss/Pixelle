from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import (
    AutoTokenizer, AutoModel, pipeline, MarianMTModel, MarianTokenizer,
    MBartForConditionalGeneration, MBartTokenizer, XLM-RoBERTaModel, XLM-RoBERTaTokenizer,
    AutoModelForSequenceClassification, AutoModelForTokenClassification
)
from sentence_transformers import SentenceTransformer
import numpy as np
import json
import logging
from dataclasses import dataclass
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests
from googletrans import Translator
import langdetect
from polyglot.detect import Detector
from polyglot.text import Text
import spacy
from spacy.language import Language

logger = logging.getLogger(__name__)

@dataclass
class LanguageInfo:
    """Information about detected language."""
    language_code: str
    language_name: str
    confidence: float
    script: Optional[str] = None
    region: Optional[str] = None

@dataclass
class MultilingualFeatures:
    """Extracted multilingual features from content."""
    content_id: str
    primary_language: LanguageInfo
    detected_languages: List[LanguageInfo]
    text_features: Dict[str, np.ndarray]
    sentiment_scores: Dict[str, Dict[str, float]]
    cultural_context: Dict[str, Any]
    translation_features: Dict[str, np.ndarray]
    code_switching_detected: bool
    regional_preferences: Dict[str, float]

class AdvancedMultilingualNLP:
    """Advanced multilingual NLP system supporting 100+ languages."""
    
    def __init__(self, 
                 device: str = "cuda" if torch.cuda.is_available() else "cpu",
                 cache_dir: str = "./model_cache",
                 supported_languages: List[str] = None):
        
        self.device = device
        self.cache_dir = cache_dir
        
        # Default supported languages (can be extended)
        self.supported_languages = supported_languages or [
            'en', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'ja', 'ko', 'zh', 'ar', 'hi', 'bn', 'ur',
            'th', 'vi', 'id', 'ms', 'tl', 'tr', 'pl', 'nl', 'sv', 'da', 'no', 'fi', 'et', 'lv',
            'lt', 'cs', 'sk', 'hu', 'ro', 'bg', 'hr', 'sl', 'el', 'he', 'fa', 'am', 'sw', 'yo',
            'ig', 'ha', 'zu', 'xh', 'af', 'st', 'sn', 'ny', 'rw', 'lg', 'ak', 'tw', 'ee', 'ga'
        ]
        
        # Language family mappings for cultural context
        self.language_families = {
            'indo_european': ['en', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'pl', 'nl', 'sv', 'da', 'no', 'fi', 'et', 'lv', 'lt', 'cs', 'sk', 'hu', 'ro', 'bg', 'hr', 'sl', 'el', 'hi', 'bn', 'ur'],
            'sino_tibetan': ['zh', 'th', 'vi'],
            'japonic': ['ja'],
            'koreanic': ['ko'],
            'afro_asiatic': ['ar', 'he', 'am', 'ha'],
            'austronesian': ['id', 'ms', 'tl'],
            'niger_congo': ['sw', 'yo', 'ig', 'zu', 'xh', 'af', 'st', 'sn', 'ny', 'rw', 'lg', 'ak', 'tw', 'ee', 'ga'],
            'turkic': ['tr'],
            'uralic': ['fi', 'et', 'hu']
        }
        
        # Initialize models
        self._load_multilingual_models()
        
        # Thread pool for async processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logger.info(f"AdvancedMultilingualNLP initialized on {device} with {len(self.supported_languages)} languages")
    
    def _load_multilingual_models(self):
        """Load all required multilingual NLP models."""
        try:
            # Universal multilingual model (XLM-RoBERTa)
            self.xlmr_tokenizer = XLM-RoBERTaTokenizer.from_pretrained("xlm-roberta-base")
            self.xlmr_model = XLM-RoBERTaModel.from_pretrained("xlm-roberta-base").to(self.device)
            
            # Multilingual BART for generation tasks
            self.mbart_tokenizer = MBartTokenizer.from_pretrained("facebook/mbart-large-50-many-to-many-mmt")
            self.mbart_model = MBartForConditionalGeneration.from_pretrained("facebook/mbart-large-50-many-to-many-mmt").to(self.device)
            
            # Language-specific models for high-quality processing
            self.language_models = {}
            self.language_tokenizers = {}
            
            # Load key language models
            key_languages = ['en', 'es', 'fr', 'de', 'zh', 'ja', 'ko', 'ar', 'hi', 'ru']
            for lang in key_languages:
                try:
                    model_name = self._get_model_name_for_language(lang)
                    if model_name:
                        self.language_models[lang] = AutoModel.from_pretrained(model_name).to(self.device)
                        self.language_tokenizers[lang] = AutoTokenizer.from_pretrained(model_name)
                        logger.info(f"Loaded model for {lang}: {model_name}")
                except Exception as e:
                    logger.warning(f"Could not load specific model for {lang}: {e}")
            
            # Multilingual sentiment analysis
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="nlptown/bert-base-multilingual-uncased-sentiment",
                device=0 if self.device == "cuda" else -1
            )
            
            # Named entity recognition
            self.ner_pipeline = pipeline(
                "ner",
                model="Davlan/bert-base-multilingual-cased-ner-hrl",
                device=0 if self.device == "cuda" else -1
            )
            
            # Text classification
            self.text_classification_pipeline = pipeline(
                "text-classification",
                model="papluca/xlm-roberta-base-language-detection",
                device=0 if self.device == "cuda" else -1
            )
            
            # Google Translate fallback
            self.google_translator = Translator()
            
            # SpaCy models for advanced NLP
            self.spacy_models = {}
            for lang in ['en', 'es', 'fr', 'de', 'zh', 'ja', 'ko']:
                try:
                    self.spacy_models[lang] = spacy.load(f"{lang}_core_web_sm")
                except:
                    logger.warning(f"SpaCy model not available for {lang}")
            
            logger.info("All multilingual models loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load multilingual models: {e}")
            raise
    
    def _get_model_name_for_language(self, language_code: str) -> Optional[str]:
        """Get the best model name for a specific language."""
        model_mapping = {
            'en': 'bert-base-uncased',
            'es': 'dccuchile/bert-base-spanish-wwm-cased',
            'fr': 'camembert-base',
            'de': 'bert-base-german-cased',
            'zh': 'bert-base-chinese',
            'ja': 'cl-tohoku/bert-base-japanese-whole-word-masking',
            'ko': 'klue/bert-base',
            'ar': 'aubmindlab/bert-base-arabertv2',
            'hi': 'microsoft/DialoGPT-medium-Hindi',
            'ru': 'DeepPavlov/rubert-base-cased'
        }
        return model_mapping.get(language_code)
    
    async def extract_multilingual_features(self, 
                                         content_id: str,
                                         text: str,
                                         metadata: Optional[Dict[str, Any]] = None) -> MultilingualFeatures:
        """Extract comprehensive multilingual features from text content."""
        
        try:
            # Detect languages
            primary_language, detected_languages = await self._detect_languages(text)
            
            # Extract text features for each detected language
            text_features = {}
            for lang_info in detected_languages:
                lang_code = lang_info.language_code
                if lang_code in self.supported_languages:
                    features = await self._extract_language_specific_features(text, lang_code)
                    text_features[lang_code] = features
            
            # Sentiment analysis in multiple languages
            sentiment_scores = await self._analyze_multilingual_sentiment(text, detected_languages)
            
            # Cultural context analysis
            cultural_context = await self._analyze_cultural_context(text, detected_languages)
            
            # Translation features
            translation_features = await self._extract_translation_features(text, detected_languages)
            
            # Detect code-switching
            code_switching_detected = len(detected_languages) > 1
            
            # Regional preferences
            regional_preferences = await self._analyze_regional_preferences(text, detected_languages)
            
            features = MultilingualFeatures(
                content_id=content_id,
                primary_language=primary_language,
                detected_languages=detected_languages,
                text_features=text_features,
                sentiment_scores=sentiment_scores,
                cultural_context=cultural_context,
                translation_features=translation_features,
                code_switching_detected=code_switching_detected,
                regional_preferences=regional_preferences
            )
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting multilingual features: {e}")
            # Return basic features
            return MultilingualFeatures(
                content_id=content_id,
                primary_language=LanguageInfo('en', 'English', 1.0),
                detected_languages=[LanguageInfo('en', 'English', 1.0)],
                text_features={},
                sentiment_scores={},
                cultural_context={},
                translation_features={},
                code_switching_detected=False,
                regional_preferences={}
            )
    
    async def _detect_languages(self, text: str) -> Tuple[LanguageInfo, List[LanguageInfo]]:
        """Detect languages in text using multiple methods."""
        try:
            detected_languages = []
            
            # Method 1: Polyglot detection
            try:
                polyglot_detector = Detector(text)
                for lang in polyglot_detector.languages:
                    if lang.confidence > 0.1:  # Filter low confidence
                        detected_languages.append(LanguageInfo(
                            language_code=lang.code,
                            language_name=lang.name,
                            confidence=lang.confidence
                        ))
            except:
                pass
            
            # Method 2: Langdetect
            try:
                langdetect_result = langdetect.detect_langs(text)
                for lang in langdetect_result:
                    detected_languages.append(LanguageInfo(
                        language_code=lang.lang,
                        language_name=self._get_language_name(lang.lang),
                        confidence=lang.prob
                    ))
            except:
                pass
            
            # Method 3: XLM-RoBERTa language detection
            try:
                inputs = self.xlmr_tokenizer(text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
                with torch.no_grad():
                    outputs = self.text_classification_pipeline(text[:512])
                    if outputs:
                        result = outputs[0]
                        detected_languages.append(LanguageInfo(
                            language_code=result['label'],
                            language_name=self._get_language_name(result['label']),
                            confidence=result['score']
                        ))
            except:
                pass
            
            # Remove duplicates and sort by confidence
            unique_languages = {}
            for lang in detected_languages:
                if lang.language_code not in unique_languages or lang.confidence > unique_languages[lang.language_code].confidence:
                    unique_languages[lang.language_code] = lang
            
            detected_languages = sorted(unique_languages.values(), key=lambda x: x.confidence, reverse=True)
            
            # Set primary language
            primary_language = detected_languages[0] if detected_languages else LanguageInfo('en', 'English', 1.0)
            
            return primary_language, detected_languages
            
        except Exception as e:
            logger.error(f"Error detecting languages: {e}")
            return LanguageInfo('en', 'English', 1.0), [LanguageInfo('en', 'English', 1.0)]
    
    def _get_language_name(self, language_code: str) -> str:
        """Get human-readable language name from code."""
        language_names = {
            'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German', 'it': 'Italian',
            'pt': 'Portuguese', 'ru': 'Russian', 'ja': 'Japanese', 'ko': 'Korean', 'zh': 'Chinese',
            'ar': 'Arabic', 'hi': 'Hindi', 'bn': 'Bengali', 'ur': 'Urdu', 'th': 'Thai',
            'vi': 'Vietnamese', 'id': 'Indonesian', 'ms': 'Malay', 'tl': 'Tagalog', 'tr': 'Turkish'
        }
        return language_names.get(language_code, language_code.upper())
    
    async def _extract_language_specific_features(self, text: str, language_code: str) -> np.ndarray:
        """Extract language-specific features using appropriate models."""
        try:
            features = []
            
            # Use XLM-RoBERTa for universal features
            inputs = self.xlmr_tokenizer(text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
            with torch.no_grad():
                outputs = self.xlmr_model(**inputs)
                # Use [CLS] token representation
                cls_features = outputs.last_hidden_state[:, 0, :].cpu().numpy().flatten()
                features.extend(cls_features)
            
            # Use language-specific model if available
            if language_code in self.language_models:
                try:
                    lang_inputs = self.language_tokenizers[language_code](text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
                    with torch.no_grad():
                        lang_outputs = self.language_models[language_code](**lang_inputs)
                        lang_cls_features = lang_outputs.last_hidden_state[:, 0, :].cpu().numpy().flatten()
                        features.extend(lang_cls_features)
                except:
                    pass
            
            # Add language-specific metadata
            features.extend([
                hash(language_code) % 1000 / 1000.0,  # Language hash
                len(text) / 1000.0,  # Text length normalized
                self._get_language_family_score(language_code)  # Language family score
            ])
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting language-specific features: {e}")
            return np.zeros(768 + 3)  # Default size
    
    def _get_language_family_score(self, language_code: str) -> float:
        """Get language family score for cultural context."""
        for family, languages in self.language_families.items():
            if language_code in languages:
                return hash(family) % 1000 / 1000.0
        return 0.0
    
    async def _analyze_multilingual_sentiment(self, text: str, detected_languages: List[LanguageInfo]) -> Dict[str, Dict[str, float]]:
        """Analyze sentiment in multiple languages."""
        try:
            sentiment_scores = {}
            
            # Analyze in primary language
            primary_lang = detected_languages[0].language_code if detected_languages else 'en'
            
            # Use multilingual sentiment pipeline
            try:
                sentiment_result = self.sentiment_pipeline(text[:512])
                if sentiment_result:
                    # Parse sentiment score (1-5 stars)
                    sentiment_text = sentiment_result[0]['label']
                    sentiment_score = float(sentiment_text.split()[0]) / 5.0  # Normalize to 0-1
                    
                    sentiment_scores[primary_lang] = {
                        'score': sentiment_score,
                        'label': sentiment_text,
                        'confidence': sentiment_result[0]['score']
                    }
            except:
                pass
            
            # Analyze in other detected languages if different
            for lang_info in detected_languages[1:]:
                try:
                    # Translate text to this language for analysis
                    translated_text = await self._translate_text(text, lang_info.language_code)
                    if translated_text:
                        sentiment_result = self.sentiment_pipeline(translated_text[:512])
                        if sentiment_result:
                            sentiment_text = sentiment_result[0]['label']
                            sentiment_score = float(sentiment_text.split()[0]) / 5.0
                            
                            sentiment_scores[lang_info.language_code] = {
                                'score': sentiment_score,
                                'label': sentiment_text,
                                'confidence': sentiment_result[0]['score']
                            }
                except:
                    pass
            
            return sentiment_scores
            
        except Exception as e:
            logger.error(f"Error analyzing multilingual sentiment: {e}")
            return {}
    
    async def _analyze_cultural_context(self, text: str, detected_languages: List[LanguageInfo]) -> Dict[str, Any]:
        """Analyze cultural context and regional preferences."""
        try:
            cultural_context = {}
            
            # Named entity recognition
            try:
                ner_result = self.ner_pipeline(text[:512])
                entities = {}
                for entity in ner_result:
                    entity_type = entity['entity_group']
                    if entity_type not in entities:
                        entities[entity_type] = []
                    entities[entity_type].append(entity['word'])
                
                cultural_context['entities'] = entities
            except:
                cultural_context['entities'] = {}
            
            # Language family analysis
            language_families = set()
            for lang_info in detected_languages:
                for family, languages in self.language_families.items():
                    if lang_info.language_code in languages:
                        language_families.add(family)
            
            cultural_context['language_families'] = list(language_families)
            
            # Regional indicators
            regional_indicators = {
                'has_emoji': any(ord(char) > 127 for char in text),
                'has_hashtags': '#' in text,
                'has_mentions': '@' in text,
                'text_direction': 'rtl' if any(ord(char) > 127 and 0x0590 <= ord(char) <= 0x05FF for char in text) else 'ltr'
            }
            cultural_context['regional_indicators'] = regional_indicators
            
            return cultural_context
            
        except Exception as e:
            logger.error(f"Error analyzing cultural context: {e}")
            return {}
    
    async def _extract_translation_features(self, text: str, detected_languages: List[LanguageInfo]) -> Dict[str, np.ndarray]:
        """Extract features from translations to different languages."""
        try:
            translation_features = {}
            
            # Translate to key languages for feature extraction
            key_languages = ['en', 'es', 'fr', 'de', 'zh', 'ja']
            
            for target_lang in key_languages:
                if target_lang not in [lang.language_code for lang in detected_languages]:
                    try:
                        translated_text = await self._translate_text(text, target_lang)
                        if translated_text:
                            # Extract features from translated text
                            features = await self._extract_language_specific_features(translated_text, target_lang)
                            translation_features[target_lang] = features
                    except:
                        pass
            
            return translation_features
            
        except Exception as e:
            logger.error(f"Error extracting translation features: {e}")
            return {}
    
    async def _translate_text(self, text: str, target_language: str) -> Optional[str]:
        """Translate text to target language."""
        try:
            # Try Google Translate
            translation = self.google_translator.translate(text, dest=target_language)
            return translation.text
        except:
            try:
                # Try MBART for supported languages
                if target_language in self.mbart_tokenizer.lang_code_to_token:
                    inputs = self.mbart_tokenizer(text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
                    inputs['forced_bos_token_id'] = self.mbart_tokenizer.lang_code_to_token[target_language]
                    
                    with torch.no_grad():
                        outputs = self.mbart_model.generate(**inputs, max_length=512)
                        translated_text = self.mbart_tokenizer.decode(outputs[0], skip_special_tokens=True)
                        return translated_text
            except:
                pass
            
            return None
    
    async def _analyze_regional_preferences(self, text: str, detected_languages: List[LanguageInfo]) -> Dict[str, float]:
        """Analyze regional preferences and cultural indicators."""
        try:
            regional_preferences = {}
            
            # Language family preferences
            for lang_info in detected_languages:
                for family, languages in self.language_families.items():
                    if lang_info.language_code in languages:
                        regional_preferences[f'family_{family}'] = lang_info.confidence
            
            # Regional content indicators
            regional_indicators = {
                'western': any(word in text.lower() for word in ['hello', 'world', 'technology', 'music']),
                'eastern': any(word in text.lower() for word in ['你好', '世界', '技术', '音乐', 'こんにちは', '안녕하세요']),
                'middle_eastern': any(word in text.lower() for word in ['مرحبا', 'عالم', 'تقنية', 'موسيقى']),
                'african': any(word in text.lower() for word in ['habari', 'dunia', 'teknolojia', 'muziki'])
            }
            
            for region, has_indicator in regional_indicators.items():
                regional_preferences[f'region_{region}'] = 1.0 if has_indicator else 0.0
            
            return regional_preferences
            
        except Exception as e:
            logger.error(f"Error analyzing regional preferences: {e}")
            return {}
    
    async def get_cross_lingual_similarity(self, text1: str, text2: str) -> float:
        """Get similarity between texts in different languages."""
        try:
            # Extract features for both texts
            features1 = await self.extract_multilingual_features("temp1", text1)
            features2 = await self.extract_multilingual_features("temp2", text2)
            
            # Use XLM-RoBERTa embeddings for cross-lingual similarity
            if features1.text_features and features2.text_features:
                # Get primary language features
                lang1 = list(features1.text_features.keys())[0]
                lang2 = list(features2.text_features.keys())[0]
                
                vec1 = features1.text_features[lang1]
                vec2 = features2.text_features[lang2]
                
                # Calculate cosine similarity
                similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
                return float(similarity)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating cross-lingual similarity: {e}")
            return 0.0
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)