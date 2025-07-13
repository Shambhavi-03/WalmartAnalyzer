import pandas as pd
import uuid
import os
import base64
from datetime import datetime, timedelta

# --- Configuration ---
LOGISTICS_DATA_PATH = os.path.join('data', 'logistics.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_LOGISTICS_DATA_PATH = os.path.join('data', 'logistics_with_alerts.csv')

# Define thresholds and keywords for Logistics alerts

# Delay thresholds (in hours)
CRITICAL_DELAY_HOURS = 48  # Shipments delayed by 2 days or more
MEDIUM_DELAY_HOURS = 24    # Shipments delayed by 1 day or more
MINOR_DELAY_HOURS = 6      # Shipments delayed by 6 hours or more

# Quantity thresholds for impact escalation
HIGH_QUANTITY_THRESHOLD = 500  # High quantity of product affected
MEDIUM_QUANTITY_THRESHOLD = 100 # Medium quantity of product affected

# Critical Statuses
CRITICAL_SHIPMENT_STATUSES = ['delayed', 'damaged', 'lost', 'stuck_in_customs', 'return_to_origin', 'exception']

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

def logistics_rule_engine(logistics_df, alerts_df):
    """
    Applies logistics-related rules to the DataFrame and logs alerts.
    Focuses on shipment delays, status changes, and deviations.
    """
    print("Running logistics rule engine...")

    if 'alert_id' not in logistics_df.columns:
        logistics_df['alert_id'] = None

    alerts_generated_count = 0
    current_time = pd.Timestamp.now()

    for index, row in logistics_df.iterrows():
        shipment_id = row.get('ShipmentID', 'N/A')
        order_id = row.get('OrderID', 'N/A')
        product_id = row.get('ProductID', 'N/A')
        quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
        status = str(row.get('Status', '')).lower()
        delay_reason = str(row.get('DelayReason', '')).lower()
        origin = row.get('OriginLocation', 'Unknown')
        destination = row.get('DestinationLocation', 'Unknown')
        carrier_id = row.get('CarrierID', 'Unknown')

        scheduled_arrival = pd.to_datetime(row.get('ScheduledArrivalTime'), errors='coerce')
        actual_arrival = pd.to_datetime(row.get('ActualArrivalTime'), errors='coerce')
        estimated_arrival = pd.to_datetime(row.get('EstimatedTimeOfArrival'), errors='coerce')
        scheduled_departure = pd.to_datetime(row.get('ScheduledDepartureTime'), errors='coerce')
        actual_departure = pd.to_datetime(row.get('ActualDepartureTime'), errors='coerce')

        context_info = f"Shipment: {shipment_id}, Order: {order_id}, Product: {product_id} ({quantity if pd.notna(quantity) else 'N/A'} units), From: {origin} to: {destination}, Carrier: {carrier_id}"
        current_alerts_for_row = []

        # Rule 1: Critical Shipment Statuses (Damaged, Lost, Customs Hold, etc.)
        if status in CRITICAL_SHIPMENT_STATUSES:
            severity = "Critical"
            alert_title = f"CRITICAL LOGISTICS ALERT: Shipment {shipment_id} Status: {status.upper()}."
            if pd.notna(quantity) and quantity >= HIGH_QUANTITY_THRESHOLD:
                alert_title += f" High impact due to {int(quantity)} units."
            
            description_suffix = f"Reason: {delay_reason if delay_reason else 'Not specified'}. {context_info}"
            alerts_df, new_alert_id = log_alert(alerts_df, f"{alert_title} {description_suffix}", "Logistics/Supply Chain", severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
            # If a critical status, no need to check for simple delays or deviations, it's already high priority.
            logistics_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
            continue

        # Rule 2: Shipment Delays (Current Delays or Anticipated Delays)
        delay_hours = 0
        is_delayed = False

        if status == 'delayed':
            is_delayed = True
            # Calculate delay based on actual vs scheduled arrival for delivered/in-transit delays
            if pd.notna(actual_arrival) and pd.notna(scheduled_arrival) and actual_arrival > scheduled_arrival:
                delay_hours = (actual_arrival - scheduled_arrival).total_seconds() / 3600
            # If still in-transit but delayed, use estimated vs scheduled arrival or current time vs scheduled arrival
            elif pd.isna(actual_arrival) and pd.notna(estimated_arrival) and pd.notna(scheduled_arrival) and estimated_arrival > scheduled_arrival:
                delay_hours = (estimated_arrival - scheduled_arrival).total_seconds() / 3600
            elif pd.isna(actual_arrival) and pd.notna(scheduled_arrival) and current_time > scheduled_arrival:
                # If scheduled arrival passed and no actual/estimated, assume delayed until current time
                delay_hours = (current_time - scheduled_arrival).total_seconds() / 3600
            
            # If there's a significant actual/estimated departure delay too
            if pd.notna(actual_departure) and pd.notna(scheduled_departure) and actual_departure > scheduled_departure:
                departure_delay = (actual_departure - scheduled_departure).total_seconds() / 3600
                delay_hours = max(delay_hours, departure_delay) # Take the larger of arrival or departure delay to capture total impact

        
        if is_delayed and delay_hours > 0:
            severity = "Low" # Default for any delay
            if delay_hours >= CRITICAL_DELAY_HOURS:
                severity = "Critical"
            elif delay_hours >= MEDIUM_DELAY_HOURS:
                severity = "Medium"
            elif delay_hours >= MINOR_DELAY_HOURS:
                severity = "Low" # Explicitly setting even if default
            
            # Escalate severity based on quantity
            if pd.notna(quantity):
                if quantity >= HIGH_QUANTITY_THRESHOLD and severity != "Critical": # High quantity can elevate to Medium or Critical
                    if severity == "Medium":
                        severity = "Critical"
                    elif severity == "Low":
                        severity = "Medium"
                elif quantity >= MEDIUM_QUANTITY_THRESHOLD and severity == "Low": # Medium quantity can elevate to Medium
                    severity = "Medium"

            alert_title = f"{severity} Delay: Shipment {shipment_id} delayed by ~{int(delay_hours)} hours."
            description_suffix = f"Reason: {delay_reason if delay_reason else 'Not specified'}. {context_info}"
            alerts_df, new_alert_id = log_alert(alerts_df, f"{alert_title} {description_suffix}", "Logistics/Supply Chain", severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1

        # Rule 3: On-time or early arrival
        if status == 'delivered' and pd.notna(actual_arrival) and pd.notna(scheduled_arrival):
            if actual_arrival <= scheduled_arrival:
                # Log success for tracking operational efficiency, low severity
                early_or_on_time_hours = (scheduled_arrival - actual_arrival).total_seconds() / 3600
                if early_or_on_time_hours > 0: # Arrived early
                    alert_title = f"Positive Logistics: Shipment {shipment_id} arrived {int(early_or_on_time_hours)} hours early."
                else: # Arrived on time
                    alert_title = f"Positive Logistics: Shipment {shipment_id} arrived on time."
                
                alerts_df, new_alert_id = log_alert(alerts_df, f"{alert_title}. {context_info}", "Logistics/Supply Chain", "Info") # Using 'Info' severity for positive alerts
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
        
        # Link alert_id(s) to the logistics data row
        if current_alerts_for_row:
            logistics_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            logistics_df.loc[index, 'alert_id'] = None

    print(f"Logistics rule engine completed. Generated {alerts_generated_count} alerts.")
    return logistics_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    logistics_df = load_data(LOGISTICS_DATA_PATH)
    if logistics_df.empty:
        print(f"Error: {LOGISTICS_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(logistics_df)} rows from {LOGISTICS_DATA_PATH}")

        # Convert date columns to datetime objects for comparison
        logistics_df['ScheduledDepartureTime'] = pd.to_datetime(logistics_df['ScheduledDepartureTime'], errors='coerce')
        logistics_df['ActualDepartureTime'] = pd.to_datetime(logistics_df['ActualDepartureTime'], errors='coerce')
        logistics_df['ScheduledArrivalTime'] = pd.to_datetime(logistics_df['ScheduledArrivalTime'], errors='coerce')
        logistics_df['ActualArrivalTime'] = pd.to_datetime(logistics_df['ActualArrivalTime'], errors='coerce')
        logistics_df['EstimatedTimeOfArrival'] = pd.to_datetime(logistics_df['EstimatedTimeOfArrival'], errors='coerce')

        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_logistics_df, updated_alerts_df = logistics_rule_engine(logistics_df.copy(), alerts_df.copy()) 

        save_data(updated_logistics_df, UPDATED_LOGISTICS_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Logistics Data (first 10 rows with alerts) ---")
        print(updated_logistics_df[['ShipmentID', 'ProductID', 'Quantity', 'Status', 'DelayReason', 'ScheduledArrivalTime', 'ActualArrivalTime', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))