import streamlit as st
import sqlite3
import pandas as pd

# পেজ কনফিগারেশন
st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide"
)

# ডেটাবেজ থেকে ফ্রড লগগুলো ফেচ করার ফাংশন
@st.cache_data(ttl=5) # প্রতি ৫ সেকেন্ডে ডেটা রিফ্রেশ করার জন্য ক্যাশ টাইম
def load_fraud_data():
    try:
        conn = sqlite3.connect("fraud_alerts.db")
        df = pd.read_sql_query("SELECT * FROM fraud_logs ORDER BY timestamp DESC", conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

# ড্যাশবোর্ড হেডার
st.title("🛡️ Real-Time Fraud Detection & LLM Alert Triage")
st.markdown("Monitor suspicious transactions caught by the pipeline and view AI-generated security analyst reports in real-time.")

# সাইডবার ফিল্টার বা স্ট্যাটাস
st.sidebar.header("Pipeline Control")
st.sidebar.success("Kafka Consumer: Active")
st.sidebar.info("Database: Connected (`fraud_alerts.db`)")

if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()

# ডেটা ಲೋড করা
df_fraud = load_fraud_data()

# মেট্রিক বা ওভারভিউ কার্ড
col1, col2, col3 = st.columns(3)
total_alerts = len(df_fraud)
total_amount = df_fraud['amount'].sum() if total_alerts > 0 else 0

col1.metric(label="🚨 Total Fraud Alerts", value=total_alerts)
col2.metric(label="💰 Total Flagged Amount", value=f"${total_amount:,.2f}")
col3.metric(label="🤖 AI Triage Status", value="Operational")

st.markdown("---")

# টেবিল এবং ডিটেইল ভিউ
if df_fraud.empty:
    st.warning("No fraud alerts found in the database yet. Run your pipeline and simulate transactions to see data here.")
else:
    st.subheader("📋 Flagged Transactions List")
    
    # ইউজার টেবিল দেখতে পাবে
    selected_tx = st.selectbox("Select Transaction ID to view LLM Security Report:", df_fraud['tx_id'].tolist())
    
    # সিলেক্ট করা ট্রানজাকশনের বিস্তারিত এবং এলএলএম রিপোর্ট দেখানো
    selected_row = df_fraud[df_fraud['tx_id'] == selected_tx].iloc[0]
    
    col_a, col_b = st.columns([1, 1])
    
    with col_a:
        st.markdown("### 📊 Transaction Details")
        st.write(f"**Transaction ID:** {selected_row['tx_id']}")
        st.write(f"**User ID:** {selected_row['user_id']}")
        st.write(f"**Amount:** ${selected_row['amount']:,.2f}")
        st.write(f"**Location:** {selected_row['location']}")
        st.write(f"**Device:** {selected_row['device']}")
        st.write(f"**Trigger Reason:** {selected_row['reason']}")
        st.write(f"**Timestamp:** {selected_row['timestamp']}")
        
    with col_b:
        st.markdown("### 🤖 LLM Security Analyst Report")
        st.info(selected_row['llm_alert'])

    st.markdown("---")
    st.subheader("📁 Raw Database Logs")
    st.dataframe(df_fraud, use_container_width=True)