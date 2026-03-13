import os
import json
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

def get_gspread_client():
    creds_json_str = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json_str:
        raise ValueError("Error: GOOGLE_CREDENTIALS environment variable not set.")
    creds_dict = json.loads(creds_json_str)
    return gspread.service_account_from_dict(creds_dict)

def is_market_open():
    """Checks if the Indian stock market is currently open."""
    now = pd.Timestamp.now(tz='Asia/Kolkata')
    if now.dayofweek > 4: # Weekend
        return False
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_start <= now <= market_end

def get_intraday(session, symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS"
    params = {"range": "1d", "interval": "5m"} 
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

        df["Timestamp"] = df["Timestamp"].dt.tz_convert("Asia/Kolkata")
        df["Timestamp"] = df["Timestamp"].dt.tz_localize(None) 
        df["Symbol"] = symbol
        return df

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.DataFrame()

def main():
    print("Starting 5-min intraday data fetch...")
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
        return

    try:
        index_ws = sh.worksheet("Index")
    except gspread.exceptions.WorksheetNotFound:
        print("Error: Could not find a worksheet named 'Index'.")
        return
        
    symbols = index_ws.col_values(1)[2:]
    symbols = [s.strip().upper() for s in symbols if s != ""]
    
    session = requests.Session()
    session.headers = {"User-Agent": "Mozilla/5.0"}

    all_data = []
    for symbol in symbols:
        df = get_intraday(session, symbol)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("No data fetched. Exiting.")
        return

    final_df = pd.concat(all_data, ignore_index=True)

    try:
        ws = sh.worksheet("Intraday_Data")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="Intraday_Data", rows="50000", cols="10")

    ws.clear()
    set_with_dataframe(ws, final_df)
    print("✅ All 5-Min Intraday Data Uploaded Successfully")

if __name__ == "__main__":
    if is_market_open():
        main()
    else:
        print("Market is currently closed. Skipping data fetch to save minutes.")
