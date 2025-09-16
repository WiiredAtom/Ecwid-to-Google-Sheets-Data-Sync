import os
import requests
import csv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import gspread
from gspread.utils import rowcol_to_a1
import pandas as pd
import numpy as np

# Load credentials from .env
load_dotenv()
store_id = os.getenv("ECWID_STORE_ID")
secret_token = os.getenv("ECWID_SECRET_TOKEN")

GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", "service_account.json")
GOOGLE_SHEET_NAME = os.getenv("NEW_SHEET_NAME", "Ecwid Data Source")
GOOGLE_ORDERS_WORKSHEET_NAME = os.getenv("GOOGLE_ORDERS_WORKSHEET_NAME", "Orders Data")
GOOGLE_LOG_WORKSHEET_NAME = os.getenv("GOOGLE_LOG_WORKSHEET_NAME", "Update Log")

INITIAL_FETCH_DATE_STR = "2025-03-17 00:00:00 +0000"

if not store_id or not secret_token:
    raise ValueError("Missing ECWID_STORE_ID or ECWID_SECRET_TOKEN in environment.")

# --- Ecwid API Setup ---
url = f"https://app.ecwid.com/api/v3/{store_id}/orders"
headers = {"Authorization": f"Bearer {secret_token}"}

ECWID_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%d %H:%M:%S",  # Corrected format
    "%Y-%m-%d"
]
OUTPUT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"

# --- FIXED: Moved function definition to the top ---
def parse_and_standardize_date(date_str):
    if not date_str:
        return None
    for fmt in ECWID_DATE_FORMATS:
        try:
            dt_obj = datetime.strptime(date_str, fmt)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            dt_obj = dt_obj.astimezone(ZoneInfo("Africa/Lagos"))
            return dt_obj
        except ValueError:
            continue
    return None
    
# --- FIXED: Moved function definition to the top ---
def normalize_option(name):
    """Normalizes product option names for consistent mapping."""
    name = name.lower()
    if any(x in name for x in ["color", "colour", "coloue", "colours", "cours"]): return "color"
    if any(x in name for x in ["size", "sizing", "sizs"]): return "size"
    if any(x in name for x in ["category", "caregory", "categories", "catgory"]): return "category"
    if any(x in name for x in ["designer"]): return "designer_category"
    if any(x in name for x in ["thickness"]): return "thickness"
    return name.replace(" ", "_")

# Define FIXED output fields for the FLATTENED (Orders Data) sheet
fieldnames_flattened = [
    "create_date", "order_number", "product_name", "category", "size", "color"
]

header_flattened = [
    "ORDER DATETIME", "ORDER NO", "PRODUCT NAME", "PRODUCT CATEGORY", "PRODUCT SIZE", "PRODUCT COLOUR"
]

ID_COLUMNS = ["product_name"]
NUMERIC_COLUMNS = ['order_number']

# --- Google Sheet Connection Setup ---
print(f"Connecting to Google Sheet '{GOOGLE_SHEET_NAME}'...")
try:
    gc = gspread.service_account(filename=GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE)
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found. Creating a new one...")
    spreadsheet = gc.create(GOOGLE_SHEET_NAME)
    print(f"Created new spreadsheet '{GOOGLE_SHEET_NAME}'.")

try:
    orders_worksheet = spreadsheet.worksheet(GOOGLE_ORDERS_WORKSHEET_NAME)
    print(f"Found existing worksheet: '{GOOGLE_ORDERS_WORKSHEET_NAME}'.")
except gspread.exceptions.WorksheetNotFound:
    print(f"Worksheet '{GOOGLE_ORDERS_WORKSHEET_NAME}' not found. Creating a new one...")
    orders_worksheet = spreadsheet.add_worksheet(title=GOOGLE_ORDERS_WORKSHEET_NAME, rows="1", cols=len(header_flattened))
    orders_worksheet.update([header_flattened])
    print(f"Created new worksheet: '{GOOGLE_ORDERS_WORKSHEET_NAME}' with headers.")

try:
    log_worksheet = spreadsheet.worksheet(GOOGLE_LOG_WORKSHEET_NAME)
    print(f"Found existing log worksheet: '{GOOGLE_LOG_WORKSHEET_NAME}'.")
except gspread.exceptions.WorksheetNotFound:
    print(f"Log worksheet '{GOOGLE_LOG_WORKSHEET_NAME}' not found. Creating a new one...")
    log_worksheet = spreadsheet.add_worksheet(title=GOOGLE_LOG_WORKSHEET_NAME, rows="1", cols=3)
    log_worksheet.update([["Timestamp (Local Time)", "Timestamp (UTC)", "Description"]])
    print(f"Created new log worksheet: '{GOOGLE_LOG_WORKSHEET_NAME}' with headers.")

# --- MODIFIED: Dynamic Fetch Logic ---
all_orders = []
new_orders_to_add = []

try:
    existing_order_numbers = orders_worksheet.col_values(fieldnames_flattened.index('order_number') + 1)[1:]

    if not existing_order_numbers or all(not num.isdigit() for num in existing_order_numbers):
        print("Worksheet is empty or contains no valid order numbers. Performing initial full fetch based on date.")
        created_from_param = INITIAL_FETCH_DATE_STR
        
        offset = 0
        limit = 100
        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "createdFrom": created_from_param
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            page = response.json()
            orders = page.get("items", [])
            all_orders.extend(orders)

            if len(orders) < limit:
                break
            offset += limit
        
        print(f"Successfully fetched {len(all_orders)} orders from initial fetch.")
        new_orders_to_add = all_orders

    else:
        last_order_number_str = max([num for num in existing_order_numbers if num.isdigit()], key=int)
        last_order_number = int(last_order_number_str)
        print(f"Found {len(existing_order_numbers)} existing orders. Highest order number is {last_order_number}.")

        limit = 100
        offset = 0
        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "sortBy": "orderNumber",
                "sortOrder": "asc"
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            page = response.json()
            orders = page.get("items", [])
            
            if not orders:
                break

            newly_fetched_orders = [order for order in orders if order.get('orderNumber') and order.get('orderNumber') > last_order_number]
            all_orders.extend(newly_fetched_orders)

            if len(orders) < limit or not newly_fetched_orders:
                break
            
            offset += limit
        
        print(f"Successfully fetched {len(all_orders)} new orders.")
        new_orders_to_add = all_orders
        
except Exception as e:
    print(f"An error occurred during the fetch process. Error: {e}")
    new_orders_to_add = []

# --- Data Normalization and Flattening ---
new_rows_for_flattened_sheet = []
print("Processing new orders and preparing data for Google Sheet...")
for order in new_orders_to_add:
    raw_create_date = order.get("createDate")
    parsed_create_date = parse_and_standardize_date(raw_create_date)
    formatted_create_date = parsed_create_date.strftime("%d-%m-%Y %H:%M:%S") if parsed_create_date else raw_create_date

    base_order_data = {
        "create_date": formatted_create_date,
        "order_number": order.get("orderNumber"),
    }

    if not order.get("items"):
        row_for_flattened = base_order_data.copy()
        row_for_flattened.update({
            "product_name": None, "category": None, "size": None, "color": None
        })
        new_rows_for_flattened_sheet.append(row_for_flattened)
        continue

    for item in order.get("items", []):
        row_for_flattened = base_order_data.copy()
        row_for_flattened.update({
            "product_name": item.get("name"),
            "category": None,
            "size": None,
            "color": None,
        })

        for opt in item.get("selectedOptions", []):
            cleaned_option_name = normalize_option(opt.get("name", ""))
            value = opt.get("value")

            if cleaned_option_name == "color":
                row_for_flattened["color"] = value
            elif cleaned_option_name == "size":
                row_for_flattened["size"] = value
            elif cleaned_option_name == "category":
                row_for_flattened["category"] = value

        row_for_flattened['category'] = row_for_flattened.get('category')
        row_for_flattened['size'] = row_for_flattened.get('size')
        row_for_flattened['color'] = row_for_flattened.get('color')

        new_rows_for_flattened_sheet.append(row_for_flattened)

# Create DataFrame
flattened_df = pd.DataFrame(new_rows_for_flattened_sheet, columns=fieldnames_flattened)

print("Applying robust cleaning for NaN/Inf/None values in data for JSON compliance...")
def clean_dataframe_for_gspread(df_to_clean, id_cols, numeric_cols, target_cols):
    df = df_to_clean.copy()
    id_cols_exist = [col for col in id_cols if col in df.columns]
    numeric_cols_exist = [col for col in numeric_cols if col in df.columns]

    for col in df.columns:
        if col in id_cols_exist:
            df[col] = df[col].astype(str).replace('<NA>', None)
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, float) and np.isnan(x)) else x)
        elif col in numeric_cols_exist:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].mask(df[col].isna(), None)
            df[col] = df[col].replace([np.inf, -np.inf], None)
        else:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, float) and np.isnan(x)) else x)
    return df[target_cols]

flattened_df = clean_dataframe_for_gspread(flattened_df, ID_COLUMNS, NUMERIC_COLUMNS, fieldnames_flattened)

# --- MODIFIED: Sort output by 'order_number' from oldest to newest ---
print("Sorting data by 'order_number' (oldest to newest)...")
flattened_df['order_number_sortable'] = pd.to_numeric(flattened_df['order_number'], errors='coerce')
flattened_df = flattened_df.sort_values(by='order_number_sortable', ascending=True).drop(columns='order_number_sortable')

# --- Google Sheet Logic for Incremental Update ---
if not flattened_df.empty:
    final_data_for_flattened_sheet = flattened_df[fieldnames_flattened].values.tolist()

    print(f"\n--- DEBUG: Sample of new data for flattened sheet (first row): {final_data_for_flattened_sheet[0]} ---")

    for r_idx, row_list in enumerate(final_data_for_flattened_sheet):
        for c_idx, val in enumerate(row_list):
            if isinstance(val, float) and (np.isnan(val) or val == np.inf or val == -np.inf):
                final_data_for_flattened_sheet[r_idx][c_idx] = None
            elif val is pd.NA:
                final_data_for_flattened_sheet[r_idx][c_idx] = None

    print(f"Appending {len(final_data_for_flattened_sheet)} new rows to worksheet '{GOOGLE_ORDERS_WORKSHEET_NAME}'...")
    orders_worksheet.append_rows(final_data_for_flattened_sheet, value_input_option='RAW')
    print("Update successful!")

else:
    print("No new orders found. Google Sheet is up to date.")

# --- Log the write activity to the Log Sheet ---
try:
    current_time_local = datetime.now().astimezone(ZoneInfo("Africa/Lagos"))
    current_time_utc = datetime.now(timezone.utc)
    num_new_records_written = len(flattened_df)

    log_entry_flattened = [
        current_time_local.strftime(OUTPUT_DATE_FORMAT),
        current_time_utc.strftime(OUTPUT_DATE_FORMAT),
        f"Incremental update completed. Added {num_new_records_written} new records to '{GOOGLE_ORDERS_WORKSHEET_NAME}'."
    ]
    log_worksheet.append_row(log_entry_flattened, value_input_option='RAW')

    print(f"Logged write activity to '{GOOGLE_LOG_WORKSHEET_NAME}'.")
except Exception as e:
    print(f"ERROR: Could not log write activity to Google Sheet: {e}")