import pandas as pd
import uuid
import os
import base64
from datetime import datetime, timedelta

# --- Configuration ---
SOCIAL_MEDIA_DATA_PATH = os.path.join('data', 'social_media_trends.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_SOCIAL_MEDIA_DATA_PATH = os.path.join('data', 'social_media_trends_with_alerts.csv')

# Define thresholds for Social Media Trends alerts

# 1. Negative Sentiment Spike
# Sentiment score typically ranges from -1 to 1, or 0 to 1. Assuming 0 to 1 where 0.5 is neutral.
NEG_SENTIMENT_CRITICAL_THRESHOLD = 0.2 # Sentiment below this is critical
NEG_SENTIMENT_MEDIUM_THRESHOLD = 0.3  # Sentiment below this is medium
NEG_SENTIMENT_LOW_THRESHOLD = 0.4     # Sentiment below this is low

# Mention count and virality score to escalate negative sentiment alerts
NEG_MENTIONS_CRITICAL_COUNT = 500   # For a trend to be critically negative, it needs significant mentions
NEG_MENTIONS_MEDIUM_COUNT = 100
NEG_VIRALITY_CRITICAL_SCORE = 0.5   # Virality indicates how fast it's spreading
NEG_VIRALITY_MEDIUM_SCORE = 0.2

# 2. Positive Virality / High Demand Signal
POS_SENTIMENT_MIN_THRESHOLD = 0.6  # Only consider positive sentiment for these alerts
POS_VIRALITY_CRITICAL_SCORE = 0.6
POS_VIRALITY_MEDIUM_SCORE = 0.3
POS_VIRALITY_LOW_SCORE = 0.1

POS_MENTIONS_CRITICAL_COUNT = 1000
POS_MENTIONS_MEDIUM_COUNT = 200
POS_MENTIONS_LOW_COUNT = 50

# 3. Low Engagement Rate (for influencer_id)
LOW_ENGAGEMENT_CRITICAL_RATE = 0.01 # Below 1% engagement
LOW_ENGAGEMENT_MEDIUM_RATE = 0.02   # Below 2% engagement
LOW_ENGAGEMENT_LOW_RATE = 0.03      # Below 3% engagement

# --- Helper Functions (re-used for consistency) ---

def load_data(filepath, columns=None):
    """
    Loads a CSV file into a pandas DataFrame.
    Handles EmptyDataError if the file is empty or has no columns.
    If 'columns' are provided, creates an empty DataFrame with those columns if the file is truly empty.
    """
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        print(f"'{filepath}' not found or is empty. Initializing empty DataFrame for it.")
        return pd.DataFrame(columns=columns)

    try:
        df = pd.read_csv(filepath)
        # Ensure all expected columns are present, even if some rows don't have data for them
        if columns:
            for col in columns:
                if col not in df.columns:
                    df[col] = None
        return df
    except pd.errors.EmptyDataError:
        print(f"'{filepath}' exists but has no columns to parse. Initializing empty DataFrame for it.")
        return pd.DataFrame(columns=columns)
    except Exception as e:
        print(f"An unexpected error occurred while loading '{filepath}': {e}")
        return pd.DataFrame(columns=columns)

def save_data(df, filepath):
    """Saves a pandas DataFrame to a CSV file."""
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def generate_alert_id():
    """Generates a shorter, URL-safe alert ID using Base64 encoding of a UUID."""
    full_uuid = uuid.uuid4()
    short_id = base64.urlsafe_b64encode(full_uuid.bytes).decode('utf-8').rstrip('=')
    return short_id

def log_alert(alerts_df, alert_title, category, severity):
    """
    Logs an alert to the alerts DataFrame and returns its ID.
    """
    alert_id = generate_alert_id()
    new_alert = pd.DataFrame([{
        'alert_id': alert_id,
        'alert_title': alert_title,
        'category': category,
        'severity': severity,
        'timestamp': pd.Timestamp.now()
    }])
    if alerts_df.empty:
        alerts_df = new_alert
    else:
        alerts_df = pd.concat([alerts_df, new_alert], ignore_index=True)
    return alerts_df, alert_id

# --- Rule Engine ---

def social_media_trends_rule_engine(social_media_df, alerts_df):
    """
    Applies social media trends rules to the DataFrame and logs alerts.
    Focuses on sentiment, virality, and engagement.
    """
    print("Running social media trends rule engine...")

    if 'alert_id' not in social_media_df.columns:
        social_media_df['alert_id'] = None

    alerts_generated_count = 0

    for index, row in social_media_df.iterrows():
        product_sku = row.get('product_sku', 'Unknown SKU')
        keyword = row.get('keyword', 'Unknown Keyword')
        mentions_count = pd.to_numeric(row.get('mentions_count'), errors='coerce')
        sentiment_score = pd.to_numeric(row.get('sentiment_score'), errors='coerce')
        platform = row.get('platform', 'Unknown Platform')
        influencer_id = row.get('influencer_id', None)
        engagement_rate = pd.to_numeric(row.get('engagement_rate'), errors='coerce')
        virality_score = pd.to_numeric(row.get('virality_score'), errors='coerce')
        campaign_mention = str(row.get('campaign_mention', 'None')).lower() # Convert to string and lower for comparison

        context_info = f"SKU: {product_sku}, Keyword: '{keyword}', Platform: {platform}"
        current_alerts_for_row = []

        # --- Rule 1: Negative Sentiment Spike ---
        if pd.notna(sentiment_score):
            if sentiment_score < NEG_SENTIMENT_CRITICAL_THRESHOLD:
                if pd.notna(mentions_count) and mentions_count >= NEG_MENTIONS_CRITICAL_COUNT or \
                   pd.notna(virality_score) and virality_score >= NEG_VIRALITY_CRITICAL_SCORE:
                    alert_title = f"CRITICAL Negative Sentiment: {context_info} (Score: {sentiment_score:.2f}, Mentions: {int(mentions_count) if pd.notna(mentions_count) else 'N/A'})"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Critical")
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1
                elif pd.notna(mentions_count) and mentions_count >= NEG_MENTIONS_MEDIUM_COUNT or \
                     pd.notna(virality_score) and virality_score >= NEG_VIRALITY_MEDIUM_SCORE:
                    alert_title = f"MEDIUM Negative Sentiment: {context_info} (Score: {sentiment_score:.2f}, Mentions: {int(mentions_count) if pd.notna(mentions_count) else 'N/A'})"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Medium")
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1
            elif sentiment_score < NEG_SENTIMENT_MEDIUM_THRESHOLD:
                if pd.notna(mentions_count) and mentions_count >= NEG_MENTIONS_MEDIUM_COUNT:
                    alert_title = f"MEDIUM Negative Sentiment: {context_info} (Score: {sentiment_score:.2f}, Mentions: {int(mentions_count) if pd.notna(mentions_count) else 'N/A'})"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Medium")
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1
            elif sentiment_score < NEG_SENTIMENT_LOW_THRESHOLD: # For sample: 0.4 sentiment is here
                alert_title = f"LOW Negative Sentiment: {context_info} (Score: {sentiment_score:.2f}, Mentions: {int(mentions_count) if pd.notna(mentions_count) else 'N/A'})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1


        # --- Rule 2: Positive Virality / High Demand Signal ---
        if pd.notna(sentiment_score) and sentiment_score >= POS_SENTIMENT_MIN_THRESHOLD:
            if pd.notna(virality_score) and virality_score >= POS_VIRALITY_CRITICAL_SCORE and \
               pd.notna(mentions_count) and mentions_count >= POS_MENTIONS_CRITICAL_COUNT:
                alert_title = f"CRITICAL Viral Trend: {context_info} (Virality: {virality_score:.2f}, Mentions: {int(mentions_count)})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif pd.notna(virality_score) and virality_score >= POS_VIRALITY_MEDIUM_SCORE and \
                 pd.notna(mentions_count) and mentions_count >= POS_MENTIONS_MEDIUM_COUNT:
                alert_title = f"MEDIUM Viral Trend: {context_info} (Virality: {virality_score:.2f}, Mentions: {int(mentions_count)})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif pd.notna(virality_score) and virality_score >= POS_VIRALITY_LOW_SCORE and \
                 pd.notna(mentions_count) and mentions_count >= POS_MENTIONS_LOW_COUNT:
                alert_title = f"LOW Viral Trend: {context_info} (Virality: {virality_score:.2f}, Mentions: {int(mentions_count)})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
        
        # --- Rule 3: Low Engagement Rate ---
        # Only check if an influencer is specified, assuming their content is tracked for engagement
        if influencer_id and influencer_id != 'None' and pd.notna(engagement_rate):
            if engagement_rate < LOW_ENGAGEMENT_CRITICAL_RATE:
                alert_title = f"CRITICAL Low Engagement: Influencer {influencer_id} for {context_info} (Rate: {engagement_rate:.2%})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif engagement_rate < LOW_ENGAGEMENT_MEDIUM_RATE: # Sample data INFL002 (0.02)
                alert_title = f"MEDIUM Low Engagement: Influencer {influencer_id} for {context_info} (Rate: {engagement_rate:.2%})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif engagement_rate < LOW_ENGAGEMENT_LOW_RATE: # Sample data INFL001 (0.03)
                alert_title = f"LOW Low Engagement: Influencer {influencer_id} for {context_info} (Rate: {engagement_rate:.2%})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 4: Campaign Underperformance/Overperformance ---
        # This rule requires historical data or defined campaign targets for proper evaluation.
        # For a single snapshot, we can flag unexpected sentiment for a mentioned campaign.
        if campaign_mention != 'none' and campaign_mention != 'nan' and pd.notna(sentiment_score):
            # Example: A campaign is mentioned, but sentiment is neutral or negative
            if sentiment_score < 0.5: # Assuming campaign aims for positive sentiment
                alert_title = f"MEDIUM Campaign Performance Alert: {campaign_mention} for {context_info} shows low sentiment ({sentiment_score:.2f})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif sentiment_score >= 0.8 and pd.notna(virality_score) and virality_score >= POS_VIRALITY_MEDIUM_SCORE:
                 alert_title = f"LOW Campaign Overperformance: {campaign_mention} for {context_info} showing strong positive trend (Sentiment: {sentiment_score:.2f}, Virality: {virality_score:.2f})"
                 alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Social Media Trends", "Low")
                 current_alerts_for_row.append(new_alert_id)
                 alerts_generated_count += 1


        # Link alert_id(s) to the social_media_df data row
        if current_alerts_for_row:
            social_media_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            social_media_df.loc[index, 'alert_id'] = None

    print(f"Social media trends rule engine completed. Generated {alerts_generated_count} alerts.")
    return social_media_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    social_media_df = load_data(SOCIAL_MEDIA_DATA_PATH)
    if social_media_df.empty:
        print(f"Error: {SOCIAL_MEDIA_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(social_media_df)} rows from {SOCIAL_MEDIA_DATA_PATH}")

        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_social_media_df, updated_alerts_df = social_media_trends_rule_engine(social_media_df.copy(), alerts_df.copy()) 

        save_data(updated_social_media_df, UPDATED_SOCIAL_MEDIA_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Social Media Trends Data (first 10 rows with alerts) ---")
        print(updated_social_media_df[['timestamp', 'product_sku', 'keyword', 'mentions_count', 'sentiment_score', 'engagement_rate', 'virality_score', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))