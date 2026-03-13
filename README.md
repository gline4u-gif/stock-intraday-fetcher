import os
import json
import time
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

def get_gspread_client():
    """Authenticates with Google Sheets using an Environment Variable securely."""
    creds_json_str = os.environ.get("GOOGLE_CREDENTIALS")
    
    if not creds_json_str:
        raise ValueError("Error: GOOGLE_CREDENTIALS environment variable not set.")
        
    creds_dict = json.loads(creds_json_str)
    return gspread.service_account_from_dict(creds_dict)

def get_intraday(session, symbol):
    """Fetches intraday data from Yahoo Finance."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS"
    params = {"range": "1d", "interval": "15m"}

    try:
        r = session.get(url, params=params)
        r.raise_for_status()
        js = r.json()
        
        result = js["chart"]["result"][0]
        ts = result["timestamp"]
        q = result["indicators"]["quote"][0]

        df = pd.DataFrame({
            "Timestamp": pd.to_datetime(ts, unit="s", utc=True),
            "Open": q["open"],
            "High": q["high"],
            "Low": q["low"],
            "Close": q["close"],
            "Volume": q["volume"]
        })

        # Convert to IST
        df["Timestamp"] = df["Timestamp"].dt.tz_convert("Asia/Kolkata")
        # Remove timezone info so it can be uploaded to Google Sheets easily
        df["Timestamp"] = df["Timestamp"].dt.tz_localize(None) 
        df["Symbol"] = symbol
        
        return df

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.DataFrame()

def main():
    print("Starting intraday data fetch pipeline...")

    # 1. Authenticate and open the Google Sheet by ID
    try:
        gc = get_gspread_client()
    except Exception as e:
        print(f"Authentication failed: {e}")
        return

    SHEET_ID = "1gJPQXhrOn03F89wcUnSViqcOjej1zSN4Qv3F3sPqNmU"
    
    try:
        sh = gc.open_by_key(SHEET_ID)
    except gspread.exceptions.APIError:
        print(f"Error: Could not access spreadsheet with ID '{SHEET_ID}'.")
        print("Did you remember to share the sheet with your Service Account email as an Editor?")
        return

    # 2. Read symbols
    try:
        index_ws = sh.worksheet("Index")
    except gspread.exceptions.WorksheetNotFound:
        print("Error: Could not find a worksheet named 'Index' in your spreadsheet.")
        return
        
    symbols = index_ws.col_values(1)[2:]
    symbols = [s.strip().upper() for s in symbols if s != ""]
    print(f"Total Symbols Found: {len(symbols)}")

    # 3. Setup session
    session = requests.Session()
    session.headers = {"User-Agent": "Mozilla/5.0"}

    # 4. Fetch all data
    all_data = []
    for symbol in symbols:
        print(f"Processing: {symbol}")
        df = get_intraday(session, symbol)

        if not df.empty:
            all_data.append(df)

        time.sleep(1) # Be kind to the API rate limits

    if not all_data:
        print("No data fetched. Exiting.")
        return

    # 5. Merge
    final_df = pd.concat(all_data, ignore_index=True)

    # 6. Upload
    try:
        ws = sh.worksheet("Intraday_Data")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="Intraday_Data", rows="50000", cols="10")

    ws.clear()
    set_with_dataframe(ws, final_df)
    print("\n✅ All Intraday Data Uploaded Successfully")

if __name__ == "__main__":
    main()
