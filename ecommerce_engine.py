import pandas as pd
import uuid
import os
import base64 # Import base64

# --- Configuration ---
ECOMMERCE_DATA_PATH = os.path.join('data', 'ecommerce.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_ECOMMERCE_DATA_PATH = os.path.join('data', 'ecommerce_with_alerts.csv')

# Define thresholds for E-commerce alerts (LOW, MEDIUM, CRITICAL)
# Note: Lower numerical value means worse performance for Conversion Rate, Online Views, Add To Cart
# Note: Higher numerical value means worse performance for Cart Abandonment Rate

# Conversion Rate (e.g., orders / views)
CONVERSION_CRITICAL_THRESHOLD = 0.008  # Below 0.8% is Critical
CONVERSION_MEDIUM_THRESHOLD = 0.012    # Between 0.8% and 1.2% is Medium
CONVERSION_LOW_THRESHOLD = 0.018     # Between 1.2% and 1.8% is Low (anything below 1.8% might be a concern)

# Cart Abandonment Rate
CART_ABANDONMENT_CRITICAL_THRESHOLD = 0.65  # Above 65% is Critical
CART_ABANDONMENT_MEDIUM_THRESHOLD = 0.60    # Between 60% and 65% is Medium
CART_ABANDONMENT_LOW_THRESHOLD = 0.55     # Between 55% and 60% is Low

# Online Views (for a single SKU/Region, arbitrary thresholds for demo, ideally dynamic)
VIEWS_CRITICAL_DROP_THRESHOLD = 300  # Below 300 views is Critical
VIEWS_MEDIUM_DROP_THRESHOLD = 600    # Between 300 and 600 views is Medium
VIEWS_LOW_DROP_THRESHOLD = 900     # Between 600 and 900 views is Low

# Add to Cart (similar to views, arbitrary thresholds for demo, ideally dynamic)
ADD_TO_CART_CRITICAL_DROP_THRESHOLD = 15   # Below 15 adds is Critical
ADD_TO_CART_MEDIUM_DROP_THRESHOLD = 40     # Between 15 and 40 adds is Medium
ADD_TO_CART_LOW_DROP_THRESHOLD = 80      # Between 40 and 80 adds is Low

# --- Helper Functions (re-used from weather_engine.py, ensuring consistency) ---

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
                    df[col] = None # Or pd.NA, depending on desired null representation
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
    # Generate a standard UUID
    full_uuid = uuid.uuid4()
    # Encode the UUID's bytes into a URL-safe Base64 string and remove padding
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

def ecommerce_rule_engine(ecommerce_df, alerts_df):
    """
    Applies e-commerce-related rules to the DataFrame and logs alerts.
    """
    print("Running e-commerce rule engine...")

    # Ensure 'alert_id' column exists in ecommerce_df, initialize with None/NaN
    if 'alert_id' not in ecommerce_df.columns:
        ecommerce_df['alert_id'] = None

    alerts_generated_count = 0

    for index, row in ecommerce_df.iterrows():
        product_sku = row.get('product_sku', 'Unknown SKU')
        region_id = row.get('region_id', 'Unknown Region')
        location_info = f"SKU: {product_sku}, Region: {region_id}"
        current_alerts_for_row = []

        # --- Rule 1: Conversion Rate Anomalies (CRITICAL, MEDIUM, LOW) ---
        if pd.notna(row['conversion_rate']):
            conversion_rate = float(row['conversion_rate'])
            if conversion_rate < CONVERSION_CRITICAL_THRESHOLD:
                alert_title = f"Critical Conversion Rate for {location_info} ({conversion_rate:.2%})"
                category = "E-commerce"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif conversion_rate < CONVERSION_MEDIUM_THRESHOLD:
                alert_title = f"Medium Conversion Rate for {location_info} ({conversion_rate:.2%})"
                category = "E-commerce"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif conversion_rate < CONVERSION_LOW_THRESHOLD:
                alert_title = f"Low Conversion Rate for {location_info} ({conversion_rate:.2%})"
                category = "E-commerce"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 2: Cart Abandonment Rate Anomalies (CRITICAL, MEDIUM, LOW) ---
        if pd.notna(row['cart_abandonment_rate']):
            cart_abandonment_rate = float(row['cart_abandonment_rate'])
            if cart_abandonment_rate > CART_ABANDONMENT_CRITICAL_THRESHOLD:
                alert_title = f"Critical Cart Abandonment for {location_info} ({cart_abandonment_rate:.2%})"
                category = "E-commerce"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif cart_abandonment_rate > CART_ABANDONMENT_MEDIUM_THRESHOLD:
                alert_title = f"Medium Cart Abandonment for {location_info} ({cart_abandonment_rate:.2%})"
                category = "E-commerce"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif cart_abandonment_rate > CART_ABANDONMENT_LOW_THRESHOLD:
                alert_title = f"Low Cart Abandonment for {location_info} ({cart_abandonment_rate:.2%})"
                category = "E-commerce"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 3: Significant Drop in Online Views (CRITICAL, MEDIUM, LOW) ---
        if pd.notna(row['online_views']):
            online_views = float(row['online_views'])
            if online_views < VIEWS_CRITICAL_DROP_THRESHOLD:
                alert_title = f"Critical Drop in Online Views for {location_info} ({int(online_views)} views)"
                category = "E-commerce"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif online_views < VIEWS_MEDIUM_DROP_THRESHOLD:
                alert_title = f"Medium Drop in Online Views for {location_info} ({int(online_views)} views)"
                category = "E-commerce"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif online_views < VIEWS_LOW_DROP_THRESHOLD:
                alert_title = f"Low Drop in Online Views for {location_info} ({int(online_views)} views)"
                category = "E-commerce"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
        
        # --- Rule 4: Significant Drop in Add to Cart (CRITICAL, MEDIUM, LOW) ---
        if pd.notna(row['add_to_cart']):
            add_to_cart = float(row['add_to_cart'])
            if add_to_cart < ADD_TO_CART_CRITICAL_DROP_THRESHOLD:
                alert_title = f"Critical Drop in Add to Cart for {location_info} ({int(add_to_cart)} adds)"
                category = "E-commerce"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif add_to_cart < ADD_TO_CART_MEDIUM_DROP_THRESHOLD:
                alert_title = f"Medium Drop in Add to Cart for {location_info} ({int(add_to_cart)} adds)"
                category = "E-commerce"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif add_to_cart < ADD_TO_CART_LOW_DROP_THRESHOLD:
                alert_title = f"Low Drop in Add to Cart for {location_info} ({int(add_to_cart)} adds)"
                category = "E-commerce"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 5: High Interest Product with No Active Promotion (LOW Severity) ---
        search_term = str(row.get('search_term', '')).lower()
        promotional_campaign_id = row.get('promotional_campaign_id')

        HIGH_INTEREST_VIEWS_THRESHOLD = 1000 
        if (('deal' in search_term or 'best' in search_term or 'discount' in search_term) and
            pd.notna(row['online_views']) and float(row['online_views']) > HIGH_INTEREST_VIEWS_THRESHOLD and
            (pd.isna(promotional_campaign_id) or promotional_campaign_id == 'None')):
            
            alert_title = f"High Interest ({search_term}) for {product_sku} in {region_id} but No Active Promotion"
            category = "E-commerce"
            severity = "Low" 
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1


        # Link alert_id(s) to the e-commerce data row
        if current_alerts_for_row:
            ecommerce_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            ecommerce_df.loc[index, 'alert_id'] = None # Explicitly set to None if no alerts

    print(f"E-commerce rule engine completed. Generated {alerts_generated_count} alerts.")
    return ecommerce_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure the 'data' directory exists
    os.makedirs('data', exist_ok=True)

    # Define the schema for the alerts table for initial creation if it's truly empty
    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    # Load existing e-commerce data
    ecommerce_df = load_data(ECOMMERCE_DATA_PATH)
    if ecommerce_df.empty:
        print(f"Error: {ECOMMERCE_DATA_PATH} not found or is empty after loading. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(ecommerce_df)} rows from {ECOMMERCE_DATA_PATH}")

        # Load existing alerts data or create an empty one with schema if file is empty
        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        # Run the rule engine
        updated_ecommerce_df, updated_alerts_df = ecommerce_rule_engine(ecommerce_df.copy(), alerts_df.copy()) 

        # Save the updated dataframes
        save_data(updated_ecommerce_df, UPDATED_ECOMMERCE_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated E-commerce Data (first 10 rows with alerts) ---")
        print(updated_ecommerce_df[['timestamp', 'product_sku', 'region_id', 'online_views', 'add_to_cart', 'online_orders', 'conversion_rate', 'cart_abandonment_rate', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))