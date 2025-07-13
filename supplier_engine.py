import pandas as pd
import uuid
import os
import base64
from datetime import datetime, timedelta

# --- Configuration ---
SUPPLIER_DATA_PATH = os.path.join('data', 'supplier.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_SUPPLIER_DATA_PATH = os.path.join('data', 'supplier_with_alerts.csv')

# Define thresholds for Supplier Performance alerts

# 1. On-Time Delivery Rate (OTD)
OTD_CRITICAL_THRESHOLD = 0.85 # Below 85% is critical
OTD_MEDIUM_THRESHOLD = 0.90   # Below 90% is medium
OTD_LOW_THRESHOLD = 0.95      # Below 95% is low (needs attention)

# 2. Defect Rate Percent
DEFECT_CRITICAL_THRESHOLD = 3.0 # Above 3.0% is critical
DEFECT_MEDIUM_THRESHOLD = 2.0   # Above 2.0% is medium
DEFECT_LOW_THRESHOLD = 1.0      # Above 1.0% is low (minor concern)

# 3. Quality Score (assuming a scale, e.g., 0-10)
QUALITY_CRITICAL_THRESHOLD = 7.0 # Below 7.0 is critical
QUALITY_MEDIUM_THRESHOLD = 8.0   # Below 8.0 is medium
QUALITY_LOW_THRESHOLD = 8.5      # Below 8.5 is low (could be better)

# 4. Lead Time Days (longer lead times are higher risk)
LEAD_TIME_CRITICAL_THRESHOLD = 14 # Greater than 14 days is critical
LEAD_TIME_MEDIUM_THRESHOLD = 10   # Greater than 10 days is medium
LEAD_TIME_LOW_THRESHOLD = 7       # Greater than 7 days is low (watch for impact)

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

def supplier_rule_engine(supplier_df, alerts_df):
    """
    Applies supplier performance rules to the DataFrame and logs alerts.
    Focuses on OTD, defect rate, quality score, and lead time.
    """
    print("Running supplier rule engine...")

    if 'alert_id' not in supplier_df.columns:
        supplier_df['alert_id'] = None

    alerts_generated_count = 0

    for index, row in supplier_df.iterrows():
        supplier_id = row.get('supplier_id', 'Unknown Supplier')
        product_sku = row.get('product_sku', 'Unknown SKU')
        
        on_time_delivery_rate = pd.to_numeric(row.get('on_time_delivery_rate'), errors='coerce')
        quality_score = pd.to_numeric(row.get('quality_score'), errors='coerce')
        defect_rate_percent = pd.to_numeric(row.get('defect_rate_percent'), errors='coerce')
        lead_time_days = pd.to_numeric(row.get('lead_time_days'), errors='coerce')
        
        context_info = f"Supplier: {supplier_id}, SKU: {product_sku}"
        current_alerts_for_row = []

        # --- Rule 1: Poor On-Time Delivery Rate ---
        if pd.notna(on_time_delivery_rate):
            if on_time_delivery_rate < OTD_CRITICAL_THRESHOLD:
                alert_title = f"CRITICAL Supplier OTD: {context_info} ({on_time_delivery_rate:.2%} OTD)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif on_time_delivery_rate < OTD_MEDIUM_THRESHOLD:
                alert_title = f"MEDIUM Supplier OTD: {context_info} ({on_time_delivery_rate:.2%} OTD)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif on_time_delivery_rate < OTD_LOW_THRESHOLD: # Supplier_C: 0.92, will trigger Medium now. Let's adjust for sample
                alert_title = f"LOW Supplier OTD: {context_info} ({on_time_delivery_rate:.2%} OTD)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 2: High Defect Rate ---
        if pd.notna(defect_rate_percent):
            if defect_rate_percent >= DEFECT_CRITICAL_THRESHOLD:
                alert_title = f"CRITICAL Supplier Defect Rate: {context_info} ({defect_rate_percent:.1f}% Defect Rate)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif defect_rate_percent >= DEFECT_MEDIUM_THRESHOLD: # Supplier_C: 2.0, will trigger Medium
                alert_title = f"MEDIUM Supplier Defect Rate: {context_info} ({defect_rate_percent:.1f}% Defect Rate)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif defect_rate_percent >= DEFECT_LOW_THRESHOLD: # Supplier_A (AF-PRO-2025): 1.5, Supplier_A (BLENDER-ULTRA): 1.2, will trigger Low
                alert_title = f"LOW Supplier Defect Rate: {context_info} ({defect_rate_percent:.1f}% Defect Rate)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 3: Low Quality Score ---
        if pd.notna(quality_score):
            if quality_score < QUALITY_CRITICAL_THRESHOLD:
                alert_title = f"CRITICAL Supplier Quality Score: {context_info} (Score: {quality_score:.1f})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif quality_score < QUALITY_MEDIUM_THRESHOLD: # Supplier_C: 7.9, will trigger Medium
                alert_title = f"MEDIUM Supplier Quality Score: {context_info} (Score: {quality_score:.1f})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif quality_score < QUALITY_LOW_THRESHOLD:
                alert_title = f"LOW Supplier Quality Score: {context_info} (Score: {quality_score:.1f})"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 4: Excessive Lead Time ---
        if pd.notna(lead_time_days):
            if lead_time_days >= LEAD_TIME_CRITICAL_THRESHOLD:
                alert_title = f"CRITICAL Supplier Lead Time: {context_info} ({int(lead_time_days)} days)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Critical")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif lead_time_days >= LEAD_TIME_MEDIUM_THRESHOLD: # Supplier_C: 10, will trigger Medium
                alert_title = f"MEDIUM Supplier Lead Time: {context_info} ({int(lead_time_days)} days)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Medium")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif lead_time_days >= LEAD_TIME_LOW_THRESHOLD: # Supplier_A (AF-PRO-2025): 7, will trigger Low
                alert_title = f"LOW Supplier Lead Time: {context_info} ({int(lead_time_days)} days)"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, "Supplier Performance", "Low")
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # Link alert_id(s) to the supplier data row
        if current_alerts_for_row:
            supplier_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            supplier_df.loc[index, 'alert_id'] = None

    print(f"Supplier rule engine completed. Generated {alerts_generated_count} alerts.")
    return supplier_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    supplier_df = load_data(SUPPLIER_DATA_PATH)
    if supplier_df.empty:
        print(f"Error: {SUPPLIER_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(supplier_df)} rows from {SUPPLIER_DATA_PATH}")

        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_supplier_df, updated_alerts_df = supplier_rule_engine(supplier_df.copy(), alerts_df.copy()) 

        save_data(updated_supplier_df, UPDATED_SUPPLIER_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Supplier Data (first 10 rows with alerts) ---")
        print(updated_supplier_df[['supplier_id', 'product_sku', 'on_time_delivery_rate', 'defect_rate_percent', 'quality_score', 'lead_time_days', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))