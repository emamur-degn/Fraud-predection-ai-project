import os

class Config:
    # Kafka Configurations
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_TRANSACTIONS = "financial-transactions"
    KAFKA_CONSUMER_GROUP_ID = "fraud-detection-group"

    # Fraud Detection Thresholds
    HIGH_RISK_AMOUNT_THRESHOLD = 5000.00  # এর বেশি অ্যামাউন্ট হলে হাই-রিস্ক ধরা হবে
    
    # OpenAI LLM Configurations
    OPENAI_MODEL = "gpt-4o-mini"
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "your-openai-api-key-here")

    # Logging format
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

config = Config()