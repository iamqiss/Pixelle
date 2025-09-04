from pydantic import BaseSettings

class Settings(BaseSettings):
	kafka_brokers: str = "localhost:9092"
	kafka_topic_interactions: str = "feeds.interactions.v1"
	redis_url: str = "redis://localhost:6379/0"
	feature_ttl_seconds: int = 3600
	service_port: int = 8088
	client_base_url: str = "http://localhost:3000"

	class Config:
		env_prefix = "OVERDRIVE_"

settings = Settings()