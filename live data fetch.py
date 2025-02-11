import asyncio
import websockets
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import nest_asyncio
import os
import time
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

# Apply fix for Jupyter Notebook
nest_asyncio.apply()

# Google Drive Authentication
print("🔑 Authenticating Google Drive...")

google_creds = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))

ga = GoogleAuth()
ga.LoadCredentialsFile(google_creds)
drive = GoogleDrive(ga)

print("✅ Google Drive Authentication Successful!")

# Deriv API Credentials
API_TOKEN = "gVqKgFDHLkSf4UQ"  # Replace with your API Token
APP_ID = "68227"  # Replace with your Deriv App ID
DERIV_API_URL = f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"

# Forex pairs to track
FOREX_PAIRS = [
    "frxAUDJPY", "frxAUDUSD", "frxEURAUD", "frxEURCAD", "frxEURCHF", "frxEURGBP", "frxEURJPY", "frxEURUSD",
    "frxGBPAUD", "frxGBPJPY", "frxGBPUSD", "frxUSDCAD", "frxUSDCHF", "frxUSDJPY", "frxAUDCAD", "frxAUDCHF",
    "frxAUDNZD", "frxEURNZD", "frxGBPCAD", "frxGBPCHF", "frxGBPNZD", "frxNZDJPY", "frxNZDUSD", "frxUSDMXN"
]

# Store price history
latest_saved_entries = {}
current_candles = {symbol: {"Open": None, "High": float('-inf'), "Low": float('inf'), "Close": None} for symbol in FOREX_PAIRS}

def save_data_to_drive(symbol, new_entry):
    try:
        filename = f"{symbol}_live_data.csv"
        df = pd.DataFrame([new_entry])
        df.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
        
        # Upload to Google Drive
        file_drive = drive.CreateFile({'title': filename})
        file_drive.SetContentFile(filename)
        file_drive.Upload()
        print(f"📁 Data saved to Google Drive: {filename}")
        
        # Cleanup local file
        os.remove(filename)
    except Exception as e:
        print(f"⚠️ Error saving data to Google Drive: {str(e)}")

def format_price_data(data):
    return {
        "Time": datetime.fromtimestamp(data['epoch'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "Symbol": data['symbol'],
        "Price": data['quote'],
        "High": max(current_candles[data['symbol']]['High'], data['quote']),
        "Low": min(current_candles[data['symbol']]['Low'], data['quote']),
        "Open": current_candles[data['symbol']]['Open'] if current_candles[data['symbol']]['Open'] else data['quote'],
        "Close": data['quote']
    }

async def authenticate(websocket):
    await websocket.send(json.dumps({"authorize": API_TOKEN}))
    response = await websocket.recv()
    auth_data = json.loads(response)
    if "error" in auth_data:
        print("❌ Authorization Error:", auth_data["error"]["message"])
        return False
    print("✅ Authorized Successfully!")
    return True

async def fetch_live_prices(websocket):
    await websocket.send(json.dumps({"ticks": FOREX_PAIRS}))
    while True:
        try:
            response = await websocket.recv()
            data = json.loads(response)
            if "tick" in data:
                tick = data["tick"]
                symbol = tick['symbol']
                new_entry = format_price_data(tick)
                save_data_to_drive(symbol, new_entry)
                print(f"📊 {new_entry}")
        except websockets.exceptions.ConnectionClosed:
            print("⚠️ Connection lost. Reconnecting...")
            await asyncio.sleep(5)
            await main()
        except Exception as e:
            print(f"⚠️ Error occurred: {e}")
            await asyncio.sleep(5)

async def main():
    while True:
        try:
            async with websockets.connect(DERIV_API_URL) as websocket:
                if not await authenticate(websocket):
                    return
                print("📱 Fetching Live Data...")
                await fetch_live_prices(websocket)
        except Exception as e:
            print(f"⚠️ Connection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        task = loop.create_task(main())
        loop.run_until_complete(task)
