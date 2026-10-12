import json
import logging
from kafka import KafkaConsumer
from openai import OpenAI
from config import config  # কনফিগারেশন ইমপোর্ট করা
from database import init_db, save_fraud_alert  # ডেটাবেজ ফাংশন ইমপোর্ট করা

# লগিং সেটআপ (Logging configuration)
logging.basicConfig(
    format=config.LOG_FORMAT,
    level=logging.INFO
)
logger = logging.getLogger("FraudPipeline")

# OpenAI ক্লায়েন্ট ইনিশিয়ালাইজ
client = OpenAI(api_key=config.OPENAI_API_KEY)

# কাফকা কনজুমার সেটআপ (Config থেকে মানগুলো নেওয়া হয়েছে)
consumer = KafkaConsumer(
    config.KAFKA_TOPIC_TRANSACTIONS,
    bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=config.KAFKA_CONSUMER_GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def check_fraud_rules(tx):
    """
    রুল-ভিত্তিক ফ্রড ডিটেকশন লজিক। 
    Config ফাইলে থাকা থ্রেশহোল্ড ব্যবহার করা হয়েছে।
    """
    amount = tx.get('amount', 0)
    location = tx.get('location')
    usual_locations = tx.get('usual_locations', [location])

    if amount > config.HIGH_RISK_AMOUNT_THRESHOLD:
        return True, f"Amount (${amount}) exceeds high-risk threshold (${config.HIGH_RISK_AMOUNT_THRESHOLD})."
    
    if location not in usual_locations:
        return True, f"Transaction initiated from an unusual geographic location: {location}."
        
    return False, "Normal"

def generate_llm_alert(tx, reason):
    """
    OpenAI LLM ব্যবহার করে সিকিউরিটি টিমের জন্য হিউম্যান-রিডেবল অ্যালার্ট তৈরি করা।
    """
    prompt = f"""
    You are an expert financial security AI. A suspicious transaction was detected.
    Transaction Details:
    - User ID: {tx.get('user_id')}
    - Amount: ${tx.get('amount')}
    - Location: {tx.get('location')}
    - Device: {tx.get('device')}
    - Trigger Reason: {reason}

    Provide a concise, professional alert for the security operations center (SOC) team explaining why this transaction is suspicious and recommend a concrete action.
    """
    
    try:
        response = client.chat.completions.create(
            model=config.OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Failed to generate LLM alert: {str(e)}")
        return "Failed to generate LLM alert due to an API error."

def start_pipeline():
    logger.info("🚀 Real-Time Fraud Detection Pipeline started. Listening for transactions...")
    
    for message in consumer:
        transaction = message.value
        tx_id = transaction.get('tx_id', 'UNKNOWN')
        logger.info(f"📥 Received Transaction ID: {tx_id}")
        
        # ফ্রড ডিটেকশন চেক
        is_fraud, reason = check_fraud_rules(transaction)
        
        if is_fraud:
            logger.warning(f"🚨 FRAUD DETECTED on TX {tx_id}! Reason: {reason}")
            
            # LLM দিয়ে অ্যালার্ট ট্রাইজ বা ব্যাখ্যা তৈরি
            alert_report = generate_llm_alert(transaction, reason)
            
            print("\n" + "="*40)
            print("🤖 LLM GENERATED SECURITY ALERT")
            print("="*40)
            print(alert_report)
            print("="*40 + "\n")
            
            # ফ্রড ট্রানজাকশন এবং এলএলএম অ্যালার্ট ডেটাবেজে সংরক্ষণ করা
            save_fraud_alert(transaction, reason, alert_report)
            
        else:
            logger.info(f"✅ Transaction {tx_id} is Safe.")

if __name__ == "__main__":
    # পাইপলাইন শুরুর আগে ডেটাবেজ ইনিশিয়েলাইজ বা টেবিল তৈরি করে নেওয়া
    init_db()
    
    try:
        start_pipeline()
    except KeyboardInterrupt:
        logger.info("\n🛑 Pipeline stopped gracefully by user.")