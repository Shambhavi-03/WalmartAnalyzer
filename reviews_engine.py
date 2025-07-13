import pandas as pd
import uuid
import os
import base64
from datetime import datetime, timedelta

# --- Configuration ---
REVIEWS_DATA_PATH = os.path.join('data', 'reviews.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_REVIEWS_DATA_PATH = os.path.join('data', 'reviews_with_alerts.csv')

# Define keywords for detecting negative sentiment in review titles
# Note: Given 'rating' and 'review_text' are N/A in sample, review_title is primary source.

CRITICAL_REVIEW_KEYWORDS = ['con artists', 'fake', 'scam', 'fraud', 'horrible', 'worst',
                            'disaster', 'never again', 'unacceptable', 'ripoff', 'stole',
                            'lied', 'deceptive']

MEDIUM_REVIEW_KEYWORDS = ['problem', 'issue', 'bug', 'delayed', 'missing', 'damaged',
                          'poor quality', 'unhappy', 'bad experience', 'defective',
                          'broken', 'difficult', 'struggle', 'misleading', 'disappointed']

LOW_REVIEW_KEYWORDS = ['disappointing', 'not great', 'concern', 'slow', 'minor issue',
                       'frustrating', 'could be better', 'average', 'mild']

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

def reviews_rule_engine(reviews_df, alerts_df):
    """
    Applies customer review rules to the DataFrame and logs alerts.
    Primarily uses review_title for sentiment detection due to N/A in other fields.
    """
    print("Running customer reviews rule engine...")
    print("WARNING: 'rating' and 'review_text' columns appear to be N/A in the sample data. "
          "Rules will primarily rely on 'review_title' keywords, which may limit accuracy.")

    if 'alert_id' not in reviews_df.columns:
        reviews_df['alert_id'] = None

    alerts_generated_count = 0

    for index, row in reviews_df.iterrows():
        source = row.get('source', 'Unknown Source')
        product_reviewed = row.get('product_reviewed', 'Unknown Product')
        review_title = str(row.get('review_title', '')).lower()
        reviewer_name = row.get('reviewer_name', 'Anonymous')
        # review_text = str(row.get('review_text', '')).lower() # N/A in sample
        # rating = pd.to_numeric(row.get('rating'), errors='coerce') # N/A in sample
        # verified_user = row.get('verified_user', False) # Boolean, default False

        context_info = f"Product: {product_reviewed}, Reviewer: {reviewer_name}, Source: {source}"
        current_alerts_for_row = []

        # --- Rule 1: Critical Negative Keywords in Review Title ---
        if any(keyword in review_title for keyword in CRITICAL_REVIEW_KEYWORDS):
            alert_title = f"CRITICAL Review: '{review_title}' - {context_info}"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Customer Reviews", "Critical")
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
            # If critical, no need to check for medium/low for this review
            reviews_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
            continue # Move to next review

        # --- Rule 2: Medium Negative Keywords in Review Title ---
        if any(keyword in review_title for keyword in MEDIUM_REVIEW_KEYWORDS):
            alert_title = f"MEDIUM Review: '{review_title}' - {context_info}"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Customer Reviews", "Medium")
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
            # If medium, no need to check for low for this review
            reviews_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
            continue # Move to next review

        # --- Rule 3: Low Negative Keywords in Review Title ---
        if any(keyword in review_title for keyword in LOW_REVIEW_KEYWORDS):
            alert_title = f"LOW Review: '{review_title}' - {context_info}"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Customer Reviews", "Low")
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
            # Continue to next review

        # Rule 4: General Positive/Neutral Reviews (Not flagged as alerts, but could be logged as 'Info' if desired)
        # For now, focusing only on 'problems', so positive/neutral reviews won't generate alerts.

        # Link alert_id(s) to the reviews data row
        if current_alerts_for_row:
            reviews_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            reviews_df.loc[index, 'alert_id'] = None

    print(f"Customer reviews rule engine completed. Generated {alerts_generated_count} alerts.")
    return reviews_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    reviews_df = load_data(REVIEWS_DATA_PATH)
    if reviews_df.empty:
        print(f"Error: {REVIEWS_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(reviews_df)} rows from {REVIEWS_DATA_PATH}")

        # Convert review_date to datetime if it's not N/A in general
        # Not critical for current rules, but good practice.
        # Reviews date format in sample is "Nov 02, 2023 09:56 PM"
        reviews_df['review_date'] = pd.to_datetime(reviews_df['review_date'], format='%b %d, %Y %I:%M %p', errors='coerce')


        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_reviews_df, updated_alerts_df = reviews_rule_engine(reviews_df.copy(), alerts_df.copy()) 

        save_data(updated_reviews_df, UPDATED_REVIEWS_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Reviews Data (first 10 rows with alerts) ---")
        print(updated_reviews_df[['review_date', 'product_reviewed', 'review_title', 'rating', 'verified_user', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))