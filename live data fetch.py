import asyncio
import websockets
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import nest_asyncio
import time
import tkinter as tk
from tkinter import filedialog
import os

# Apply fix for Jupyter Notebook
nest_asyncio.apply()

# Create global variable for selected directory
SAVE_DIRECTORY = None

# Deriv API Credentials
API_TOKEN = ""  # Replace with your API Token
APP_ID = ""  # Replace with your Deriv App ID
DERIV_API_URL = f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"

# Trading settings
TRADE_AMOUNT = 10  # Amount per trade
RSI_OVERBOUGHT = 70  # RSI level to consider overbought (SELL)
RSI_OVERSOLD = 30  # RSI level to consider oversold (BUY)
SMA_PERIOD = 14  # Moving average period
MIN_PRICE_CHANGE = 0.0001  # Minimum price change required to trigger trade

# Forex pairs to track
FOREX_PAIRS = [
    "frxAUDJPY", "frxAUDUSD", "frxEURAUD", "frxEURCAD", "frxEURCHF", "frxEURGBP", "frxEURJPY", "frxEURUSD",
    "frxGBPAUD", "frxGBPJPY", "frxGBPUSD", "frxUSDCAD", "frxUSDCHF", "frxUSDJPY", "frxAUDCAD", "frxAUDCHF",
    "frxAUDNZD", "frxEURNZD", "frxGBPCAD", "frxGBPCHF", "frxGBPNZD", "frxNZDJPY", "frxNZDUSD", "frxUSDMXN"
]

# Store price history
latest_saved_entries = {}  # Store last saved entry per pair
current_candles = {symbol: {"Open": None, "High": float('-inf'), "Low": float('inf'), "Close": None, "Symbol": None, "Ask": None, "Bid": None, "Epoch": None, "Pip_Size": None} for symbol in FOREX_PAIRS}

def select_save_directory():
    """Function to set a fixed directory (for server environments)"""
    global SAVE_DIRECTORY
    SAVE_DIRECTORY = "/opt/render/project/src/data"  # Change this path as needed
    os.makedirs(SAVE_DIRECTORY, exist_ok=True)
    print(f"📂 Selected directory for saving files: {SAVE_DIRECTORY}")
    return True

def save_data_to_csv(symbol, new_entry):
    global latest_saved_entries, SAVE_DIRECTORY
    try:
        # Create file path inside the selected directory
        filename = os.path.join(SAVE_DIRECTORY, f"{symbol}_live_data.csv")
        df = pd.DataFrame([new_entry])
        
        # Check if the entry is different from the last saved entry
        if symbol not in latest_saved_entries or latest_saved_entries[symbol] != new_entry:
            # Save directly to CSV file in append mode
            df.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
            latest_saved_entries[symbol] = new_entry
            print(f"📁 Data saved to {filename}")
    except PermissionError:
        print(f"⚠️ Permission denied for {symbol}. Please close any open CSV files.")
        time.sleep(1)
    except Exception as e:
        print(f"⚠️ Error saving data for {symbol}: {str(e)}")
        time.sleep(1)

async def authenticate(websocket):
    """ Authenticate with API Token """
    request = {"authorize": API_TOKEN}
    await websocket.send(json.dumps(request))
    response = await websocket.recv()
    auth_data = json.loads(response)

    if "error" in auth_data:
        print("❌ Authorization Error:", auth_data["error"]["message"])
        return False
    else:
        print("✅ Authorized Successfully!")
        return True

async def fetch_live_prices(websocket):
    """ Fetch live forex prices for multiple pairs """
    request = {"ticks": FOREX_PAIRS}
    await websocket.send(json.dumps(request))

    while True:
        try:
            response = await websocket.recv()
            data = json.loads(response)

            if "tick" in data:
                tick = data["tick"]
                symbol = tick['symbol']
                epoch_time = tick['epoch']
                price = tick['quote']
                ask_price = tick.get('ask', None)
                bid_price = tick.get('bid', None)
                pip_size = tick.get('pip_size', None)

                # Convert to readable time
                readable_time = datetime.fromtimestamp(epoch_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                # Set open price if not already set
                if current_candles[symbol]["Open"] is None:
                    current_candles[symbol]["Open"] = price

                # Update current candle
                current_candles[symbol]["High"] = max(current_candles[symbol]["High"], price)
                current_candles[symbol]["Low"] = min(current_candles[symbol]["Low"], price)
                current_candles[symbol]["Close"] = price
                current_candles[symbol]["Ask"] = ask_price
                current_candles[symbol]["Bid"] = bid_price
                current_candles[symbol]["Epoch"] = epoch_time
                current_candles[symbol]["Pip_Size"] = pip_size

                new_entry = {
                    "Time": readable_time,
                    "Symbol": symbol,
                    "Ask": ask_price,
                    "Bid": bid_price,
                    "Epoch": epoch_time,
                    "Pip_Size": pip_size,
                    "Open": current_candles[symbol]["Open"],
                    "Price": price,
                    "High": current_candles[symbol]["High"],
                    "Low": current_candles[symbol]["Low"],
                    "Close": current_candles[symbol]["Close"]
                }
                
                # Save data to CSV
                save_data_to_csv(symbol, new_entry)
                print(f"📊 {readable_time} | {symbol} | Price: {price} | Ask: {ask_price} | Bid: {bid_price} | Open: {current_candles[symbol]['Open']} | High: {current_candles[symbol]['High']} | Low: {current_candles[symbol]['Low']} | Close: {current_candles[symbol]['Close']}")

        except websockets.exceptions.ConnectionClosed:
            print("⚠️ Connection lost. Reconnecting...")
            await asyncio.sleep(5)
            await main()
        except Exception as e:
            print(f"⚠️ Error occurred: {e}")
            await asyncio.sleep(5)

async def main():
    # First, ask for directory selection
    if not select_save_directory():
        print("Program cannot continue without selecting a directory.")
        return

    while True:
        try:
            async with websockets.connect(DERIV_API_URL) as websocket:
                if not await authenticate(websocket):
                    return
                print("\n📱 Fetching Live Data...")
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
