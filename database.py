import sqlite3
import logging

logger = logging.getLogger("FraudPipeline")

DB_NAME = "fraud_alerts.db"

def init_db():
    """
    ডেটাবেজ এবং টেবিল তৈরি করা, যদি আগে থেকে না থাকে।
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fraud_logs (
                tx_id TEXT PRIMARY KEY,
                user_id TEXT,
                amount REAL,
                location TEXT,
                device TEXT,
                reason TEXT,
                llm_alert TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")

def save_fraud_alert(tx, reason, llm_alert):
    """
    ফ্রড ট্রানজাকশন এবং এলএলএম অ্যালার্ট ডেটাবেজে সংরক্ষণ করা।
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO fraud_logs (tx_id, user_id, amount, location, device, reason, llm_alert)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            tx.get('tx_id'),
            tx.get('user_id'),
            tx.get('amount'),
            tx.get('location'),
            tx.get('device'),
            reason,
            llm_alert
        ))
        conn.commit()
        conn.close()
        logger.info(f"💾 Fraud alert for transaction {tx.get('tx_id')} saved to database.")
    except Exception as e:
        logger.error(f"Failed to save alert to database: {str(e)}")