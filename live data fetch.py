import asyncio
import websockets
import json
import os
import pandas as pd
from datetime import datetime, timezone
import nest_asyncio
from google.auth.transport.requests import Request
from google.auth import exceptions
from google_auth_oauthlib.flow import InstalledAppFlow
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from dotenv import load_dotenv
import google_auth_oauthlib


# Apply fix for Jupyter Notebook
nest_asyncio.apply()

# Load environment variables from .env file
load_dotenv()

# Google Drive Authentication
print("🔑 Authenticating Google Drive...")

# Fetch the credentials from the environment variables
google_creds = {
    "client_id": os.getenv("GOOGLE_CLIENT_ID"),
    "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
    "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
    "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_X509_CERT_URL"),
    "project_id": os.getenv("GOOGLE_PROJECT_ID")
}

# Save credentials to a temporary file
with open("client_secrets.json", "w") as f:
    json.dump(google_creds, f)

ga = GoogleAuth()

# Function to authenticate with Google Drive using google-auth
def authenticate_google_drive():
    creds = None
    if os.path.exists('token.json'):
        creds = GoogleAuth.LoadCredentialsFile('token.json')
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', ['https://www.googleapis.com/auth/drive.file'])
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

ga.credentials = authenticate_google_drive()
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
    except Exception as e:
        print(f"⚠️ Error saving data to Google Drive: {str(e)}")

def format_price_data(data):
    symbol = data['symbol']
    price = data['quote']
    current_data = current_candles[symbol]

    # Update the high, low, and close prices
    current_data["High"] = max(current_data["High"], price)
    current_data["Low"] = min(current_data["Low"], price)
    current_data["Close"] = price

    # Set Open price only once when the candle starts
    if current_data["Open"] is None:
        current_data["Open"] = price

    # Format entry
    return {
        "Time": datetime.fromtimestamp(data['epoch'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "Symbol": symbol,
        "Price": price,
        "High": current_data["High"],
        "Low": current_data["Low"],
        "Open": current_data["Open"],
        "Close": current_data["Close"]
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
