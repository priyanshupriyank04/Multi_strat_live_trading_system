#  Step 1: Import Required Libraries
import os                # For environment variables and file handling
import time              # For adding delays where needed
import datetime          # To handle timestamps
import pandas as pd      # For working with dataframes
import math              # For working with math related functions
import psycopg2          # PostgreSQL database connection
import logging           # For structured logging
from kiteconnect import KiteConnect  # Zerodha API connection


# Logging Setup
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logging.info(" Required libraries imported successfully.")

#  Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#  API Credentials
API_KEY = "api_key"  #  Replace with your actual API key
API_SECRET = "secret_key"  # Replace with your actual API secret
ACCESS_TOKEN_FILE = "access_token.txt"

#  Initialize KiteConnect
kite = KiteConnect(api_key=API_KEY)


def get_access_token():
    """
    Checks if the access token exists and is valid. If not, prompts the user to manually enter a new one.
    """
    #  Step 1: Check if access_token.txt exists
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE, "r") as file:
            access_token = file.read().strip()
            kite.set_access_token(access_token)
            logging.info(" Found existing access token. Attempting authentication...")

            #  Step 2: Validate access token
            try:
                profile = kite.profile()  #  API call to validate token
                logging.info(f"API Authentication Successful! User: {profile['user_name']}")
                return access_token  # ✅Return the valid token
            except Exception as e:
                logging.warning(f" Invalid/Expired Access Token: {e}")
    
    #  Step 3: If token is invalid or file does not exist, ask the user for a new one
    logging.info(" Fetching new access token...")

    request_token_url = kite.login_url()
    logging.info(f" Go to this URL, authorize, and retrieve the request token: {request_token_url}")
    
    request_token = input(" Paste the request token here: ").strip()

    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]

        # 🔹 Step 4: Save the new access token
        with open(ACCESS_TOKEN_FILE, "w") as file:
            file.write(access_token)

        logging.info(" New access token saved successfully!")
        return access_token
    except Exception as e:
        logging.error(f" Failed to generate access token: {e}")
        return None

#  Get Access Token
access_token = get_access_token()

if access_token:
    logging.info(" API is now authenticated and ready to use!")
else:
    logging.error(" API authentication failed. Please check credentials and try again.")

from psycopg2 import sql

#  Database Configuration
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "admin123"
DB_HOST = "localhost"
DB_PORT = "5432"

#  Connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True  # Enable autocommit mode
        return conn
    except Exception as e:
        logging.error(f" Failed to connect to database: {e}")
        return None
connect_to_db()

#Fetch nifty index price 
def get_nifty50_price():
    """
    Fetches the real-time Nifty 50 index price with retry logic.
    """
    retries = 5
    for attempt in range(retries):
        try:
            nifty_data = kite.ltp("NSE:NIFTY 50")
            nifty_price = nifty_data["NSE:NIFTY 50"]["last_price"]
            logging.info(f"Fetched Nifty 50 Index Price: {nifty_price}")
            return nifty_price
        except Exception as e:
            logging.warning(f" Attempt {attempt + 1}/{retries}: Error fetching Nifty 50 price: {e}")
            time.sleep(1)  # Wait before retrying
    
    logging.error(" Failed to fetch Nifty 50 price after retries.")
    return None

# Get all available option instruments
option_instruments = kite.instruments("NFO")


#Fetch nifty 50 option price 
def get_nifty50_option_price(option_token):
    """
    Fetches the real-time price of the Nifty 50 option contract from Zerodha API.
    :param option_token: Instrument token of the Nifty 50 option contract.
    :return: Last traded price (LTP) of the option contract.
    """
    try:
        logging.info(f"Fetching LTP for token: {option_token}")
        
        #  Fetch LTP from Zerodha API
        option_data = kite.ltp(option_token)

        #  Log full API response to check the structure
        logging.info(f" Full LTP API response: {option_data}")

        #  Use token as a string directly, without "NFO:"
        token_str = str(option_token)

        if token_str in option_data:
            option_price = option_data[token_str]["last_price"]
            logging.info(f" Fetched Nifty 50 Option Price: {option_price}")
            return option_price
        else:
            logging.error(f" LTP response does not contain expected token: {option_token}")
            return None

    except Exception as e:
        logging.error(f" Error fetching Nifty 50 option price: {e}")
        return None  # Return None if fetching fails


import datetime
import logging

#Fetch nifty custom weekly expiry date
def get_custom_nifty_expiry():
    """
    Returns a manually mapped expiry date based on today's date for Nifty 50 contracts.
    """
    today = datetime.date.today()



    if today <= datetime.date(today.year, 6, 5):
        return datetime.date(today.year, 6, 5)  # Weekly expiry
    elif today <= datetime.date(today.year, 6, 12):
        return datetime.date(today.year, 6, 12)  # Weekly expiry
    elif today <= datetime.date(today.year, 6, 19):
        return datetime.date(today.year, 6, 19)  # Monthly expiry
    elif today <= datetime.date(today.year, 6, 26):
        return datetime.date(today.year, 6, 26)  # Weekly expiry
    elif today <= datetime.date(today.year, 7, 3):
        return datetime.date(today.year, 7, 3)  # Weekly expiry
    else:
        logging.error(" No predefined expiry date available for current date.")
        return None


# Find the nearest OTM CE contract based on the Nifty index price
def get_nearest_otm_ce_contract(nifty_index_price):
    try:
        expiry = get_custom_nifty_expiry()
        logging.info(f"Expiry date is {expiry}")
        if not expiry:
            return None, None

        ce_options = [
            inst for inst in option_instruments
            if inst["name"] == "NIFTY"
            and inst["instrument_type"] == "CE"
            and inst["expiry"] == expiry
        ]

        if not ce_options:
            logging.warning(" No CE contracts found for selected expiry.")
            return None, None

        #  Nearest OTM CE: Next strike ABOVE the current price
        otm_ce_strike = int(math.ceil(nifty_index_price / 50) * 50)

        best_ce = min(ce_options, key=lambda x: abs(x["strike"] - otm_ce_strike))

        ltp = get_nifty50_option_price(best_ce["instrument_token"])
        logging.info(f" CE OTM Contract: {best_ce['tradingsymbol']} | Token: {best_ce['instrument_token']} | 💰 LTP: {ltp}")
        return best_ce["tradingsymbol"], best_ce["instrument_token"]

    except Exception as e:
        logging.error(f" Error in get_nearest_otm_ce_contract: {e}")
        return None, None

# Find the nearest OTM PE contract based on the Nifty index price
def get_nearest_otm_pe_contract(nifty_index_price):
    try:
        expiry = get_custom_nifty_expiry()
        if not expiry:
            return None, None

        pe_options = [
            inst for inst in option_instruments
            if inst["name"] == "NIFTY"
            and inst["instrument_type"] == "PE"
            and inst["expiry"] == expiry
        ]

        if not pe_options:
            logging.warning(" No PE contracts found for selected expiry.")
            return None, None

        #  Nearest OTM PE: Next strike BELOW the current price
        otm_pe_strike = int(math.floor(nifty_index_price / 50) * 50)

        best_pe = min(pe_options, key=lambda x: abs(x["strike"] - otm_pe_strike))

        ltp = get_nifty50_option_price(best_pe["instrument_token"])
        logging.info(f" PE OTM Contract: {best_pe['tradingsymbol']} | Token: {best_pe['instrument_token']} | 💰 LTP: {ltp}")
        return best_pe["tradingsymbol"], best_pe["instrument_token"]

    except Exception as e:
        logging.error(f" Error in get_nearest_otm_pe_contract: {e}")
        return None, None

def get_nearest_otm_ce_pe_tables(nifty_price):
    """
    Fetch nearest OTM CE & PE contracts based on latest NIFTY price
    and return their respective 5-minute OHLC table names and metadata.
    """
    ce_symbol, ce_token = get_nearest_otm_ce_contract(nifty_price)
    pe_symbol, pe_token = get_nearest_otm_pe_contract(nifty_price)

    if not ce_symbol or not pe_symbol:
        logging.error(" Failed to fetch nearest OTM CE/PE contracts.")
        return None

    return {
        "CE": {
            "symbol": ce_symbol,
            "token": ce_token,
            "table_5min": f"{ce_symbol.lower()}_ohlc_5min"
        },
        "PE": {
            "symbol": pe_symbol,
            "token": pe_token,
            "table_5min": f"{pe_symbol.lower()}_ohlc_5min"
        }
    }

def create_nearest_otm_ohlc_tables(ce_symbol, pe_symbol):
    """
    Create 5-minute OHLC tables for nearest OTM CE and PE contracts.
    Drops existing tables and recreates with required structure for:
    - ADX and DI indicators
    - CBOE (Stoch RSI + Market Index + Odds) indicator
    """

    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()

            # Define table names
            ce_table_5min = f"{ce_symbol.lower()}_ohlc_5min"
            pe_table_5min = f"{pe_symbol.lower()}_ohlc_5min"

            # Drop existing 5-min tables if exist
            for table in [ce_table_5min, pe_table_5min]:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                logging.info(f" Dropped existing table if present: {table}")

            # Create CE 5-min table
            cur.execute(f"""
                CREATE TABLE {ce_table_5min} (
                    timestamp TIMESTAMPTZ PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    ema_22 FLOAT,
                    ema_33 FLOAT,
                    hl2 FLOAT,
                    atr FLOAT,
                    initial_upper_bar FLOAT,
                    initial_lower_bar FLOAT,
                    supertrend_upper FLOAT,
                    supertrend_lower FLOAT,
                    os FLOAT,
                    spt FLOAT,
                    max_channel FLOAT,
                    min_channel FLOAT,
                    supertrend_avg FLOAT,
                    adx FLOAT,
                    di_plus FLOAT,
                    di_minus FLOAT

                );
            """)

            # Create PE 5-min table
            cur.execute(f"""
                CREATE TABLE {pe_table_5min} (
                    timestamp TIMESTAMPTZ PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    ema_22 FLOAT,
                    ema_33 FLOAT,
                    hl2 FLOAT,
                    atr FLOAT,
                    initial_upper_bar FLOAT,
                    initial_lower_bar FLOAT,
                    supertrend_upper FLOAT,
                    supertrend_lower FLOAT,
                    os FLOAT,
                    spt FLOAT,
                    max_channel FLOAT,
                    min_channel FLOAT,
                    supertrend_avg FLOAT,
                    adx FLOAT,
                    di_plus FLOAT,
                    di_minus FLOAT
                );
            """)

            conn.commit()
            logging.info(" Created fresh 5-minute OHLC tables for Nearest OTM CE/PE successfully.")

            cur.close()
            conn.close()

        except Exception as e:
            logging.error(f" Failed to create 5-minute OHLC tables for Nearest OTM CE/PE: {e}")



#  Step 1: Fetch Latest Nifty 50 Price
nifty_price = get_nifty50_price()

#  Step 2: Get Nearest OTM CE/PE Contract Details
nearest_contracts = get_nearest_otm_ce_pe_tables(nifty_price)

if nearest_contracts:
    print("\n Nearest OTM CE Contract Details:")
    print(nearest_contracts["CE"])

    print("\n Nearest OTM PE Contract Details:")
    print(nearest_contracts["PE"])

    #  Step 3: Create Fresh OHLC Tables for Nearest OTM CE & PE
    create_nearest_otm_ohlc_tables(
        nearest_contracts["CE"]["symbol"],
        nearest_contracts["PE"]["symbol"]
    )

else:
    print(" Failed to fetch nearest CE/PE contracts. Exiting...")

#Fetch CE contracts in range nifty-500,nifty+500
def get_ce_contracts(nifty_index_price):
    """
    Fetches all CE contracts in the range of NIFTY_INDEX +/- 500 with step of 50
    Returns list of dict with symbol and instrument_token
    """
    try:
        expiry = get_custom_nifty_expiry()
        if not expiry:
            return []

        ce_options = [
            inst for inst in option_instruments
            if inst["name"] == "NIFTY"
            and inst["instrument_type"] == "CE"
            and inst["expiry"] == expiry
        ]

        if not ce_options:
            logging.warning(" No CE contracts found for selected expiry.")
            return []

        lower_limit = int(math.floor((nifty_index_price - 500) / 50) * 50)
        upper_limit = int(math.floor((nifty_index_price + 500) / 50) * 50)

        selected_contracts = []

        for strike in range(lower_limit, upper_limit + 1, 50):
            for option in ce_options:
                if option["strike"] == strike:
                    selected_contracts.append({
                        "symbol": option["tradingsymbol"],
                        "token": option["instrument_token"]
                    })

        logging.info(f" Total CE Contracts Found: {len(selected_contracts)}")
        return selected_contracts

    except Exception as e:
        logging.error(f" Error in get_ce_contracts: {e}")
        return []

#Fetch PE contracts in the range nifty-500,nifty+500
def get_pe_contracts(nifty_index_price):
    """
    Fetches all PE contracts in the range of NIFTY_INDEX +/- 500 with step of 50
    Returns list of dict with symbol and instrument_token
    """
    try:
        expiry = get_custom_nifty_expiry()
        if not expiry:
            return []

        pe_options = [
            inst for inst in option_instruments
            if inst["name"] == "NIFTY"
            and inst["instrument_type"] == "PE"
            and inst["expiry"] == expiry
        ]

        if not pe_options:
            logging.warning(" No PE contracts found for selected expiry.")
            return []

        lower_limit = int(math.floor((nifty_index_price - 500) / 50) * 50)
        upper_limit = int(math.floor((nifty_index_price + 500) / 50) * 50)

        selected_contracts = []

        for strike in range(lower_limit, upper_limit + 1, 50):
            for option in pe_options:
                if option["strike"] == strike:
                    selected_contracts.append({
                        "symbol": option["tradingsymbol"],
                        "token": option["instrument_token"]
                    })

        logging.info(f" Total PE Contracts Found: {len(selected_contracts)}")
        return selected_contracts

    except Exception as e:
        logging.error(f" Error in get_pe_contracts: {e}")
        return []

#Fetch all the contracts 
def fetch_contracts(nifty_index_price):
    """
    Fetches all CE and PE contracts in the range of NIFTY_INDEX +/- 500 with step of 50
    Returns a dictionary with lists of CE and PE contracts
    """
    return {
        "ce_contracts": get_ce_contracts(nifty_index_price),
        "pe_contracts": get_pe_contracts(nifty_index_price)
    }

nifty_price = get_nifty50_price()
contracts = fetch_contracts(nifty_price)

# Print to see the result
print("CE Contracts:")
for contract in contracts['ce_contracts']:
    print(contract)

print("PE Contracts:")
for contract in contracts['pe_contracts']:
    print(contract)

#List of market holidays
#  Market Holidays for 2025
MARKET_HOLIDAYS = {
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25"
}

#  Fetch last trading day's OHLC data for a single table (dynamic)
def fetch_last_trading_day_ohlc_for_table(table_name, instrument_token, interval):
    """
    Fetches last trading day's OHLC data for the given table and instrument token.
    Used when switching nearest ITM CE/PE contracts dynamically.

    Args:
        table_name (str): Name of the database table (example: nifty25apr23850ce_ohlc_1min)
        instrument_token (int): Instrument token of the option contract
        interval (str): "minute" for 1-min data, "5minute" for 5-min data
    """

    try:
        conn = connect_to_db()
        if not conn:
            return None

        cur = conn.cursor()

        now = datetime.datetime.now()
        last_trading_day = now - datetime.timedelta(days=1)

        #  Ensure we pick previous working day (skip weekends and holidays)
        while last_trading_day.strftime("%Y-%m-%d") in MARKET_HOLIDAYS or last_trading_day.weekday() in [5, 6]:
            last_trading_day -= datetime.timedelta(days=1)

        from_date = last_trading_day.strftime("%Y-%m-%d 09:15:00")
        to_date = last_trading_day.strftime("%Y-%m-%d 15:30:00")

        logging.info(f" Fetching last trading day's {interval} data for token {instrument_token}: {from_date} to {to_date}")

        # Fetch Historical Data
        historical_data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )

        if historical_data:
            df = pd.DataFrame(historical_data)
            df["timestamp"] = pd.to_datetime(df["date"])
            df.drop(columns=["date"], inplace=True)

            for _, row in df.iterrows():
                cur.execute(f"""
                    INSERT INTO {table_name} (timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING;
                """, (
                    row["timestamp"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"]
                ))

            conn.commit()
            logging.info(f"Last trading day's {interval} data inserted into {table_name} successfully.")
        else:
            logging.warning(f" No historical data found for token {instrument_token}.")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error fetching last trading day's {interval} data for table {table_name}: {e}")
        return None



#  Fetch & Merge Today's Data for a Specific 5-Min Table (CE or PE)
def fetch_and_merge_ohlc_for_table(table_name, instrument_token, interval="5minute"):
    """
    Fetch and merge today's OHLC data (including last trading day's) for a 5-minute table.
    """

    conn = connect_to_db()
    if not conn:
        return

    cur = conn.cursor()

    # Step 1: Fetch Last Trading Day's Data
    fetch_last_trading_day_ohlc_for_table(table_name, instrument_token, interval)

    # Step 2: Fetch Today's Data up to current completed candle
    now = datetime.datetime.now()
    from_date = now.replace(hour=9, minute=15, second=0).strftime("%Y-%m-%d %H:%M:%S")
    to_date = now.replace(minute=(now.minute // 5) * 5, second=0).strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f" Fetching today's {interval} data for {table_name} from {from_date} to {to_date}")

    try:
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )

        if data:
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["date"])
            df.drop(columns=["date"], inplace=True)

            for _, row in df.iterrows():
                cur.execute(f"""
                    INSERT INTO {table_name} (timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING;
                """, (
                    row["timestamp"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"]  
                ))

            conn.commit()
            logging.info(f" Today's {interval} data merged into {table_name} successfully!")

        else:
            logging.warning(f" No today's {interval} data fetched for {table_name}")

    except Exception as e:
        logging.error(f" Error fetching today's {interval} data for {table_name}: {e}")

    finally:
        cur.close()
        conn.close()

fetch_and_merge_ohlc_for_table(nearest_contracts["CE"]["table_5min"], nearest_contracts["CE"]["token"], "5minute")
fetch_and_merge_ohlc_for_table(nearest_contracts["PE"]["table_5min"], nearest_contracts["PE"]["token"], "5minute")

import datetime

def create_nearest_otm_contracts_table():
    """
    Drops (if exists) and creates the nearest_otm_contracts table cleanly,
    tailored for 5-minute ADX-based strategies (no 1-min tables).
    """
    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()

        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS nearest_otm_contracts;")
        logging.info(" Dropped existing nearest_otm_contracts table.")

        # Create fresh table with only 5-min references
        cur.execute("""
            CREATE TABLE nearest_otm_contracts (
                ce_symbol TEXT,
                ce_token BIGINT,
                ce_table_5min TEXT,
                pe_symbol TEXT,
                pe_token BIGINT,
                pe_table_5min TEXT,
                update_timestamp TIMESTAMPTZ
            );
        """)
        conn.commit()

        logging.info(" Created fresh nearest_otm_contracts table successfully!")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error creating nearest_otm_contracts table: {e}")


create_nearest_otm_contracts_table()

def update_nearest_otm_contracts():
    """
    Fetches the latest Nifty price, finds nearest OTM CE/PE contracts,
    and updates the nearest_otm_contracts table with only 5min table info.
    """
    try:
        # Step 1: Fetch live Nifty price
        nifty_price = get_nifty50_price()
        if nifty_price is None:
            logging.warning(" Failed to fetch Nifty 50 price while updating nearest OTM contracts.")
            return

        # Step 2: Get nearest OTM CE/PE contracts
        nearest_otm = get_nearest_otm_ce_pe_tables(nifty_price)
        if nearest_otm is None:
            logging.warning(" Failed to fetch nearest OTM CE/PE contracts.")
            return

        current_timestamp = datetime.datetime.now()

        # Step 3: Connect to DB
        conn = connect_to_db()
        if not conn:
            logging.error(" Database connection failed during nearest OTM update.")
            return
        
        cur = conn.cursor()

        # Step 4: Clear old row
        cur.execute("TRUNCATE TABLE nearest_otm_contracts;")

        # Step 5: Insert new row
        cur.execute("""
            INSERT INTO nearest_otm_contracts (
                ce_symbol, ce_token, ce_table_5min,
                pe_symbol, pe_token, pe_table_5min,
                update_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            nearest_otm["CE"]["symbol"],
            nearest_otm["CE"]["token"],
            nearest_otm["CE"]["table_5min"],
            nearest_otm["PE"]["symbol"],
            nearest_otm["PE"]["token"],
            nearest_otm["PE"]["table_5min"],
            current_timestamp
        ))

        conn.commit()
        cur.close()
        conn.close()

        logging.info(f" Nearest OTM CE/PE contracts updated successfully at {current_timestamp}!")

    except Exception as e:
        logging.error(f" Error while updating nearest OTM contracts: {e}")


update_nearest_otm_contracts()

#  Initialize Current CE and PE Tokens from nearest_otm_contracts
def initialize_current_tokens():
    """
    Fetches the latest CE and PE tokens from the nearest_otm_contracts table
    and initializes global variables: current_ce_token and current_pe_token.
    """
    global current_ce_token, current_pe_token

    try:
        conn = connect_to_db()
        if not conn:
            logging.error(" Failed to connect to DB while initializing current tokens.")
            return False

        cur = conn.cursor()

        cur.execute("""
            SELECT ce_token, pe_token 
            FROM nearest_otm_contracts
            ORDER BY update_timestamp DESC
            LIMIT 1;
        """)
        result = cur.fetchone()

        if result and len(result) == 2:
            current_ce_token = result[0]
            current_pe_token = result[1]
            logging.info(f" Initialized Current CE Token: {current_ce_token}, PE Token: {current_pe_token}")
            success = True
        else:
            logging.error(" No data found in nearest_otm_contracts table to initialize tokens.")
            success = False

        cur.close()
        conn.close()
        return success

    except Exception as e:
        logging.error(f" Error initializing current tokens: {e}")
        return False


#  Call this during setup
initialize_current_tokens()

import pandas as pd
import numpy as np

def calculate_ema_for_table(table_name: str, length: int):
    """
    Calculates Exponential Moving Average (EMA) of 'close' for a given length
    and updates the specified table's corresponding column (ema_<length>).
    """
    column_name = f"ema_{length}"
    try:
        conn = connect_to_db()
        if not conn:
            logging.error(" DB connection failed for EMA calculation.")
            return

        cur = conn.cursor()

        #  Step 1: Ensure EMA column exists
        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s;
        """, (table_name, column_name))
        result = cur.fetchone()

        if not result:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} FLOAT;")
            logging.info(f" Added missing column: {column_name} to table {table_name}")

        #  Step 2: Fetch close prices
        cur.execute(f"SELECT timestamp, close FROM {table_name} ORDER BY timestamp ASC;")
        rows = cur.fetchall()
        if not rows:
            logging.warning(f" No data found in table {table_name} for EMA-{length} calculation.")
            return

        df = pd.DataFrame(rows, columns=["timestamp", "close"])

        #  Step 3: Calculate EMA
        df[column_name] = df["close"].ewm(span=length, adjust=False).mean()

        #  Step 4: Update table
        for _, row in df.iterrows():
            cur.execute(
                f"""
                UPDATE {table_name}
                SET {column_name} = %s
                WHERE timestamp = %s;
                """,
                (
                    round(row[column_name], 4) if not pd.isna(row[column_name]) else None,
                    row["timestamp"]
                )
            )

        conn.commit()
        logging.info(f" EMA-{length} updated for table {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error in calculate_ema_for_table({table_name}, {length}): {e}")


calculate_ema_for_table(nearest_contracts["CE"]["table_5min"], length=22)
calculate_ema_for_table(nearest_contracts["CE"]["table_5min"], length=33)
calculate_ema_for_table(nearest_contracts["PE"]["table_5min"], length=22)
calculate_ema_for_table(nearest_contracts["PE"]["table_5min"], length=33)

import pandas as pd
import numpy as np

def calculate_adx_for_table(table_name: str, period: int = 2):
    """
    Calculates ADX, DI+, and DI− using Wilder's smoothing (matching Pine Script)
    and updates them in the given OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            logging.error(" DB connection failed for ADX calculation.")
            return

        cur = conn.cursor()

        # Fetch OHLC data
        cur.execute(f"SELECT timestamp, open, high, low, close FROM {table_name} ORDER BY timestamp ASC;")
        rows = cur.fetchall()
        if not rows:
            logging.warning(f" No data found in table {table_name} for ADX calculation.")
            return

        # Create DataFrame
        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close"])
        df["prev_close"] = df["close"].shift(1)
        df["prev_high"] = df["high"].shift(1)
        df["prev_low"] = df["low"].shift(1)

        # True Range
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - df["prev_close"]).abs()
        tr3 = (df["low"] - df["prev_close"]).abs()
        df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # Directional Movement
        up_move = df["high"] - df["prev_high"]
        down_move = df["prev_low"] - df["low"]
        df["+dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        df["-dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        # Initialize smoothing columns
        df["tr_smooth"] = 0.0
        df["+dm_smooth"] = 0.0
        df["-dm_smooth"] = 0.0

        # Fix 1: Use iloc[0:period] instead of [1:period+1] for PineScript match
        df.loc[period, "tr_smooth"] = df["tr"].iloc[0:period].sum()
        df.loc[period, "+dm_smooth"] = df["+dm"].iloc[0:period].sum()
        df.loc[period, "-dm_smooth"] = df["-dm"].iloc[0:period].sum()

        # Fix 2: Initialize first row for safe smoothing
        df.loc[0, "tr_smooth"] = df.loc[0, "tr"]
        df.loc[0, "+dm_smooth"] = df.loc[0, "+dm"]
        df.loc[0, "-dm_smooth"] = df.loc[0, "-dm"]

        # Wilder smoothing
        for i in range(period + 1, len(df)):
            df.loc[i, "tr_smooth"] = df.loc[i-1, "tr_smooth"] - (df.loc[i-1, "tr_smooth"] / period) + df.loc[i, "tr"]
            df.loc[i, "+dm_smooth"] = df.loc[i-1, "+dm_smooth"] - (df.loc[i-1, "+dm_smooth"] / period) + df.loc[i, "+dm"]
            df.loc[i, "-dm_smooth"] = df.loc[i-1, "-dm_smooth"] - (df.loc[i-1, "-dm_smooth"] / period) + df.loc[i, "-dm"]

        # DI+, DI-
        df["di_plus"] = 100 * df["+dm_smooth"] / df["tr_smooth"]
        df["di_minus"] = 100 * df["-dm_smooth"] / df["tr_smooth"]

        # DX and ADX
        df["dx"] = 100 * (df["di_plus"] - df["di_minus"]).abs() / (df["di_plus"] + df["di_minus"])
        df["adx"] = df["dx"].rolling(window=period).mean()

        # Update DB
        for _, row in df.iterrows():
            cur.execute(f"""
                UPDATE {table_name}
                SET 
                    adx = %s,
                    di_plus = %s,
                    di_minus = %s
                WHERE timestamp = %s;
            """, (
                round(row["adx"], 4) if not pd.isna(row["adx"]) else None,
                round(row["di_plus"], 4) if not pd.isna(row["di_plus"]) else None,
                round(row["di_minus"], 4) if not pd.isna(row["di_minus"]) else None,
                row["timestamp"]
            ))

        conn.commit()
        logging.info(f" ADX, DI+ and DI− (PineScript match) updated for table {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error in calculate_adx_for_table({table_name}): {e}")


calculate_adx_for_table(nearest_contracts["CE"]["table_5min"], period=2)
calculate_adx_for_table(nearest_contracts["PE"]["table_5min"], period=2)

#  Calculate HL2 (High + Low) / 2 for a specific table
def calculate_hl2_for_table(table_name):
    """
    Calculates HL2 (High + Low) / 2 and updates for all rows in a specific 5min OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return
        
        cur = conn.cursor()

        #  Update HL2 for all rows (no WHERE condition)
        cur.execute(f"""
            UPDATE {table_name}
            SET hl2 = (high + low) / 2;
        """)
        conn.commit()

        logging.info(f" HL2 recalculated and updated for all rows in {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating HL2 for {table_name}: {e}")


calculate_hl2_for_table(nearest_contracts["CE"]["table_5min"])
calculate_hl2_for_table(nearest_contracts["PE"]["table_5min"])

#  ATR Settings
ATR_LENGTH = 10
ATR_MULTIPLIER = 3

def calculate_atr_for_table(table_name):
    """
    Calculates ATR (Average True Range) using RMA logic
    and updates 'atr' column for the full table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return
        
        cur = conn.cursor()

        logging.info(f" Calculating ATR for table: {table_name}")

        # Fetch full OHLC data (not only where atr is null)
        cur.execute(f"""
            SELECT timestamp, high, low, close
            FROM {table_name}
            ORDER BY timestamp;
        """)
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found for ATR calculation in {table_name}")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'high', 'low', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['true_range'] = 0.0
        df['atr'] = 0.0

        for i in range(1, len(df)):
            high = df.iloc[i]['high']
            low = df.iloc[i]['low']
            prev_close = df.iloc[i - 1]['close']
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            df.at[df.index[i], 'true_range'] = tr

        for i in range(len(df)):
            if i == 0:
                df.at[df.index[i], 'atr'] = 0.0
            elif i < ATR_LENGTH:
                df.at[df.index[i], 'atr'] = df['true_range'][:i+1].mean()
            else:
                prev_atr = df.iloc[i-1]['atr']
                tr = df.iloc[i]['true_range']
                df.at[df.index[i], 'atr'] = ((prev_atr * (ATR_LENGTH - 1)) + tr) / ATR_LENGTH

        df['atr'] *= ATR_MULTIPLIER
        df.drop(columns=['true_range'], inplace=True)

        update_query = f"UPDATE {table_name} SET atr = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['atr'], row['timestamp']))

        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"ATR calculation & update completed for {table_name}")

    except Exception as e:
        logging.error(f" Error calculating ATR for {table_name}: {e}")

#  Apply ATR calculation only to Nearest OTM Contracts
calculate_atr_for_table(nearest_contracts["CE"]["table_5min"])
calculate_atr_for_table(nearest_contracts["PE"]["table_5min"])

#  Calculate Initial Upper Band for Single Table
def calculate_initial_upper_band_for_table(table_name):
    """
    Calculate Initial Upper Band (hl2 + atr) for a specific 5-min OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()

        logging.info(f" Calculating Initial Upper Band for Table: {table_name}")

        # Check if hl2 & atr columns exist
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'hl2'
            );
        """, (table_name,))
        hl2_exists = cur.fetchone()[0]

        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'atr'
            );
        """, (table_name,))
        atr_exists = cur.fetchone()[0]

        if not hl2_exists or not atr_exists:
            logging.warning(f" Columns 'hl2' or 'atr' missing in {table_name}. Skipping...")
            return

        cur.execute(f"""
            UPDATE {table_name}
            SET initial_upper_bar = hl2 + atr;
        """)

        conn.commit()
        logging.info(f" Initial Upper Band calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Initial Upper Band for {table_name}: {e}")

#  Calculate Initial Lower Band for Single Table
def calculate_initial_lower_band_for_table(table_name):
    """
    Calculate Initial Lower Band (hl2 - atr) for a specific 5-min OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()

        logging.info(f" Calculating Initial Lower Band for Table: {table_name}")

        # Check if hl2 & atr columns exist
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'hl2'
            );
        """, (table_name,))
        hl2_exists = cur.fetchone()[0]

        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'atr'
            );
        """, (table_name,))
        atr_exists = cur.fetchone()[0]

        if not hl2_exists or not atr_exists:
            logging.warning(f" Columns 'hl2' or 'atr' missing in {table_name}. Skipping...")
            return

        cur.execute(f"""
            UPDATE {table_name}
            SET initial_lower_bar = hl2 - atr;
        """)

        conn.commit()
        logging.info(f" Initial Lower Band calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Initial Lower Band for {table_name}: {e}")

#  Now Apply only to CE and PE contracts
calculate_initial_upper_band_for_table(nearest_contracts['CE']['table_5min'])
calculate_initial_upper_band_for_table(nearest_contracts['PE']['table_5min'])

calculate_initial_lower_band_for_table(nearest_contracts['CE']['table_5min'])
calculate_initial_lower_band_for_table(nearest_contracts['PE']['table_5min'])


# Calculate Supertrend Upper Band
def calculate_supertrend_upper_for_table(table_name):
    """
    Calculate Dynamic Supertrend Upper Band (Based on Initial Upper Band & Previous Close)
    for a specific 5-min OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()
        logging.info(f" Calculating Supertrend Upper Band for Table: {table_name}")

        # Check if required columns exist
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'initial_upper_bar'
            );
        """, (table_name,))
        upper_exists = cur.fetchone()[0]

        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'close'
            );
        """, (table_name,))
        close_exists = cur.fetchone()[0]

        if not upper_exists or not close_exists:
            logging.warning(f" Columns 'initial_upper_bar' or 'close' missing in {table_name}. Skipping...")
            return

        cur.execute(f"""
            SELECT timestamp, initial_upper_bar, close 
            FROM {table_name} 
            ORDER BY timestamp;
        """)
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Supertrend Upper calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'initial_upper_bar', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['supertrend_upper'] = 0.0

        for i in range(len(df)):
            if i == 0:
                df.at[i, 'supertrend_upper'] = df.at[i, 'initial_upper_bar']
            else:
                prev_upper = df.at[i - 1, 'supertrend_upper']
                initial_upper = df.at[i, 'initial_upper_bar']
                prev_close = df.at[i - 1, 'close']

                if prev_close < prev_upper:
                    df.at[i, 'supertrend_upper'] = min(initial_upper, prev_upper)
                else:
                    df.at[i, 'supertrend_upper'] = initial_upper

        df['timestamp'] = df['timestamp'].astype(str)
        df['supertrend_upper'] = df['supertrend_upper'].astype(float)

        update_query = f"UPDATE {table_name} SET supertrend_upper = %s WHERE timestamp = %s;"
        for _, row in df.iterrows():
            cur.execute(update_query, (row['supertrend_upper'], row['timestamp']))

        conn.commit()
        logging.info(f" Supertrend Upper Band updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Supertrend Upper Band for {table_name}: {e}")

#  Calculate Supertrend Lower Band
def calculate_supertrend_lower_for_table(table_name):
    """
    Calculate Dynamic Supertrend Lower Band (Based on Initial Lower Band & Previous Close)
    for a specific 5-min OHLC table.
    """
    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()
        logging.info(f" Calculating Supertrend Lower Band for Table: {table_name}")

        # Check if required columns exist
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'initial_lower_bar'
            );
        """, (table_name,))
        lower_exists = cur.fetchone()[0]

        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'close'
            );
        """, (table_name,))
        close_exists = cur.fetchone()[0]

        if not lower_exists or not close_exists:
            logging.warning(f" Columns 'initial_lower_bar' or 'close' missing in {table_name}. Skipping...")
            return

        cur.execute(f"""
            SELECT timestamp, initial_lower_bar, close 
            FROM {table_name} 
            ORDER BY timestamp;
        """)
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Supertrend Lower calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'initial_lower_bar', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['supertrend_lower'] = 0.0

        for i in range(len(df)):
            if i == 0:
                df.at[i, 'supertrend_lower'] = df.at[i, 'initial_lower_bar']
            else:
                prev_lower = df.at[i - 1, 'supertrend_lower']
                initial_lower = df.at[i, 'initial_lower_bar']
                prev_close = df.at[i - 1, 'close']

                if prev_close >= prev_lower:
                    df.at[i, 'supertrend_lower'] = max(initial_lower, prev_lower)
                else:
                    df.at[i, 'supertrend_lower'] = initial_lower

        df['timestamp'] = df['timestamp'].astype(str)
        df['supertrend_lower'] = df['supertrend_lower'].astype(float)

        update_query = f"UPDATE {table_name} SET supertrend_lower = %s WHERE timestamp = %s;"
        for _, row in df.iterrows():
            cur.execute(update_query, (row['supertrend_lower'], row['timestamp']))

        conn.commit()
        logging.info(f" Supertrend Lower Band updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Supertrend Lower Band for {table_name}: {e}")

#  Apply Only to CE and PE tables
calculate_supertrend_upper_for_table(nearest_contracts['CE']['table_5min'])
calculate_supertrend_upper_for_table(nearest_contracts['PE']['table_5min'])

calculate_supertrend_lower_for_table(nearest_contracts['CE']['table_5min'])
calculate_supertrend_lower_for_table(nearest_contracts['PE']['table_5min'])

def calculate_oscillation_state_for_table(table_name):
    """
    Calculate Oscillation State (os) for a specific 5-min OHLC table.
    """

    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()
        logging.info(f" Calculating Oscillation State for Table: {table_name}")

        # Check required columns
        cur.execute(f"""
        SELECT 
            EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='supertrend_upper'),
            EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='supertrend_lower'),
            EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='close')
        """, (table_name, table_name, table_name))
        upper_exists, lower_exists, close_exists = cur.fetchone()

        if not (upper_exists and lower_exists and close_exists):
            logging.warning(f" Required columns missing in {table_name}. Skipping...")
            return

        cur.execute(f"""
            SELECT timestamp, close, supertrend_upper, supertrend_lower 
            FROM {table_name} 
            ORDER BY timestamp;
        """)
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Oscillation State calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'close', 'supertrend_upper', 'supertrend_lower'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['os'] = 0  # Default Bearish

        for i in range(len(df)):
            close = df.at[i, 'close']
            upper_band = df.at[i, 'supertrend_upper']
            lower_band = df.at[i, 'supertrend_lower']

            if close > upper_band:
                df.at[i, 'os'] = 1  # Bullish
            elif close < lower_band:
                df.at[i, 'os'] = 0  # Bearish
            else:
                df.at[i, 'os'] = df.at[i - 1, 'os'] if i > 0 else 0

        df['timestamp'] = df['timestamp'].astype(str)
        df['os'] = df['os'].astype(int)

        update_query = f"UPDATE {table_name} SET os = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['os'], row['timestamp']))

        conn.commit()
        logging.info(f" Oscillation State calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Oscillation State for {table_name}: {e}")



def calculate_supertrend_pivot_for_table(table_name):
    """
    Calculate Supertrend Pivot (spt) for a specific 5-min OHLC table.
    """

    try:
        conn = connect_to_db()
        if not conn:
            return
        
        cur = conn.cursor()
        logging.info(f" Calculating Supertrend Pivot (SPT) for Table: {table_name}")

        # Check if required columns exist
        cur.execute(f"""
            SELECT 
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='os'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='supertrend_upper'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='supertrend_lower')
        """, (table_name, table_name, table_name))
        os_exists, upper_exists, lower_exists = cur.fetchone()

        if not (os_exists and upper_exists and lower_exists):
            logging.warning(f" Required columns missing in {table_name}. Skipping...")
            return

        cur.execute(f"SELECT timestamp, os, supertrend_upper, supertrend_lower FROM {table_name} ORDER BY timestamp;")
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Supertrend Pivot calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'os', 'supertrend_upper', 'supertrend_lower'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['spt'] = df.apply(lambda row: row['supertrend_lower'] if row['os'] == 1 else row['supertrend_upper'], axis=1)

        df['timestamp'] = df['timestamp'].astype(str)
        df['spt'] = df['spt'].astype(float)

        update_query = f"UPDATE {table_name} SET spt = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['spt'], row['timestamp']))

        conn.commit()
        logging.info(f" Supertrend Pivot (SPT) calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Supertrend Pivot (SPT) for {table_name}: {e}")


#  Calculate Oscillation State for CE and PE (not for NIFTY anymore)
calculate_oscillation_state_for_table(nearest_contracts['CE']['table_5min'])
calculate_oscillation_state_for_table(nearest_contracts['PE']['table_5min'])

#  Calculate Supertrend Pivot (SPT) for CE and PE (not for NIFTY anymore)
calculate_supertrend_pivot_for_table(nearest_contracts['CE']['table_5min'])
calculate_supertrend_pivot_for_table(nearest_contracts['PE']['table_5min'])


def calculate_max_channel_for_table(table_name):
    """
    Calculate Max Channel using Oscillation State (os), Close Price, and Supertrend Pivot (spt)
    for a specific 5-min OHLC table.
    """

    try:
        conn = connect_to_db()
        if not conn:
            return
        
        cur = conn.cursor()
        logging.info(f" Calculating Max Channel for Table: {table_name}")

        # Check if required columns exist
        cur.execute(f"""
            SELECT 
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='os'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='close'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='spt')
        """, (table_name, table_name, table_name))
        os_exists, close_exists, spt_exists = cur.fetchone()

        if not (os_exists and close_exists and spt_exists):
            logging.warning(f" Required columns missing in {table_name}. Skipping...")
            return

        cur.execute(f"SELECT timestamp, os, close, spt FROM {table_name} ORDER BY timestamp;")
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Max Channel calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'os', 'close', 'spt'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['max_channel'] = 0.0

        for i in range(len(df)):
            close = df.at[i, 'close']
            os = df.at[i, 'os']
            spt = df.at[i, 'spt']

            if i == 0:
                df.at[i, 'max_channel'] = close
            else:
                prev_max_channel = df.at[i - 1, 'max_channel']
                prev_os = df.at[i - 1, 'os']

                if close > spt:
                    df.at[i, 'max_channel'] = max(prev_max_channel, close)
                elif os == 1:
                    df.at[i, 'max_channel'] = max(close, prev_max_channel)
                else:
                    df.at[i, 'max_channel'] = min(spt, prev_max_channel)

        df['timestamp'] = df['timestamp'].astype(str)
        df['max_channel'] = df['max_channel'].astype(float)

        update_query = f"UPDATE {table_name} SET max_channel = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['max_channel'], row['timestamp']))

        conn.commit()
        logging.info(f" Max Channel calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Max Channel for {table_name}: {e}")


def calculate_min_channel_for_table(table_name):
    """
    Calculate Min Channel using Oscillation State (os), Close Price, and Supertrend Pivot (spt)
    for a specific 5-min OHLC table.
    """

    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()
        logging.info(f" Calculating Min Channel for Table: {table_name}")

        # Check if required columns exist
        cur.execute(f"""
            SELECT 
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='os'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='close'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='spt')
        """, (table_name, table_name, table_name))
        os_exists, close_exists, spt_exists = cur.fetchone()

        if not (os_exists and close_exists and spt_exists):
            logging.warning(f" Required columns missing in {table_name}. Skipping...")
            return

        cur.execute(f"SELECT timestamp, os, close, spt FROM {table_name} ORDER BY timestamp;")
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Min Channel calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'os', 'close', 'spt'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['min_channel'] = 0.0

        for i in range(len(df)):
            close = df.at[i, 'close']
            os = df.at[i, 'os']
            spt = df.at[i, 'spt']

            if i == 0:
                df.at[i, 'min_channel'] = close
            else:
                prev_min_channel = df.at[i - 1, 'min_channel']
                prev_os = df.at[i - 1, 'os']

                if close < spt:
                    df.at[i, 'min_channel'] = min(prev_min_channel, close)
                elif os == 0:
                    df.at[i, 'min_channel'] = min(close, prev_min_channel)
                else:
                    df.at[i, 'min_channel'] = max(spt, prev_min_channel)

        df['timestamp'] = df['timestamp'].astype(str)
        df['min_channel'] = df['min_channel'].astype(float)

        update_query = f"UPDATE {table_name} SET min_channel = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['min_channel'], row['timestamp']))

        conn.commit()
        logging.info(f" Min Channel calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Min Channel for {table_name}: {e}")

#  Calculate Max Channel for Nearest CE & PE Only (skip NIFTY)
calculate_max_channel_for_table(nearest_contracts['CE']['table_5min'])
calculate_max_channel_for_table(nearest_contracts['PE']['table_5min'])

#  Calculate Min Channel for Nearest CE & PE Only (skip NIFTY)
calculate_min_channel_for_table(nearest_contracts['CE']['table_5min'])
calculate_min_channel_for_table(nearest_contracts['PE']['table_5min'])

def calculate_supertrend_avg_for_table(table_name):
    """
    Calculate Supertrend Average Channel using Max and Min Channels
    for a specific 5-min OHLC table.
    """

    try:
        conn = connect_to_db()
        if not conn:
            return

        cur = conn.cursor()
        logging.info(f" Calculating Supertrend Average Channel for Table: {table_name}")

        # Check if required columns exist
        cur.execute(f"""
            SELECT 
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='max_channel'),
                EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name='min_channel')
        """, (table_name, table_name))
        max_exists, min_exists = cur.fetchone()

        if not (max_exists and min_exists):
            logging.warning(f" Required columns missing in {table_name}. Skipping...")
            return

        cur.execute(f"SELECT timestamp, max_channel, min_channel FROM {table_name} ORDER BY timestamp;")
        rows = cur.fetchall()

        if not rows:
            logging.warning(f" No data found in {table_name} for Supertrend Average calculation.")
            return

        df = pd.DataFrame(rows, columns=['timestamp', 'max_channel', 'min_channel'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['supertrend_avg'] = (df['max_channel'] + df['min_channel']) / 2

        df['timestamp'] = df['timestamp'].astype(str)
        df['supertrend_avg'] = df['supertrend_avg'].astype(float)

        update_query = f"UPDATE {table_name} SET supertrend_avg = %s WHERE timestamp = %s;"

        for _, row in df.iterrows():
            cur.execute(update_query, (row['supertrend_avg'], row['timestamp']))

        conn.commit()
        logging.info(f" Supertrend Average Channel calculated & updated successfully for {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error updating Supertrend Average Channel for {table_name}: {e}")

#  Calculate Supertrend Average Channel for Nearest CE & PE Only (skip NIFTY)
calculate_supertrend_avg_for_table(nearest_contracts['CE']['table_5min'])
calculate_supertrend_avg_for_table(nearest_contracts['PE']['table_5min'])


def get_5min_table_for_token(token):
    """
    Returns the correct 5-min OHLC table name based on token (CE or PE).
    """
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT ce_token, ce_table_5min, pe_token, pe_table_5min FROM nearest_otm_contracts LIMIT 1;")
    result = cur.fetchone()

    ce_token, ce_table_5min, pe_token, pe_table_5min = result

    cur.close()
    conn.close()

    if token == ce_token:
        return ce_table_5min
    elif token == pe_token:
        return pe_table_5min
    else:
        return None

def get_symbol_from_token(token):
    """
    Returns the symbol (trading symbol) based on token (CE or PE).
    """
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT ce_token, ce_symbol, pe_token, pe_symbol FROM nearest_otm_contracts LIMIT 1;")
    result = cur.fetchone()

    ce_token, ce_symbol, pe_token, pe_symbol = result

    cur.close()
    conn.close()

    if token == ce_token:
        return ce_symbol
    elif token == pe_token:
        return pe_symbol
    else:
        return None
    

from kiteconnect import KiteConnect





#  Initialize this global variable once before starting infinite loop
last_5min_processed = None


#Setting up live websocket connection, tick by tick handling and aggregation
from kiteconnect import KiteTicker
import logging
import time
import os
import datetime
from collections import defaultdict, deque

#  Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#  Zerodha API Credentials
API_KEY = "8re7mjcm2btaozwf"  # Replace with your API key

#  Fetch access token dynamically from the file
with open("access_token.txt", "r") as f:
    ACCESS_TOKEN = f.read().strip()

#  Dynamically Prepare All Instrument Tokens for Subscription
# From fetched contracts
INSTRUMENT_TOKENS = [256265]  # NIFTY Index Token

# Add all CE + PE contract tokens
INSTRUMENT_TOKENS += [contract['token'] for contract in contracts['ce_contracts'] + contracts['pe_contracts']]

logging.info(f" All Tokens to Subscribe for Live Ticks: {INSTRUMENT_TOKENS}")

#  Initialize KiteTicker WebSocket
kws = KiteTicker(API_KEY, ACCESS_TOKEN)


#  Store Realtime OHLC for Current Candle for Each Symbol
ohlc_data = {}  
# Structure → {symbol: {'timestamp': current, 'open': o, 'high': h, 'low': l, 'close': c}}

#  Kill Existing WebSocket Processes (Optional - Safe Cleanup)
def kill_existing_websockets():
    try:
        os.system("pkill -f kiteconnect")  
        logging.info(" Existing WebSocket instances killed successfully.")
    except Exception as e:
        logging.error(f" Error while killing existing WebSocket processes: {e}")

#  Tick Buffers for 1-Minute OHLC Calculation
tick_buffer = defaultdict(lambda: defaultdict(deque))
# Structure → {symbol: {minute: deque([tick1, tick2, ...])}}

#  Tick Buffers for 5-Minute OHLC Calculation
tick_buffer_5min = defaultdict(lambda: deque())

def get_verified_5min_volume(token, start_time_str):
    """
    Fetches the historical 5-minute candle volume using Kite API for a specific token and timestamp.
    """
    try:
        from_datetime = datetime.datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
        to_datetime = from_datetime + datetime.timedelta(minutes=5)

        candles = kite.historical_data(
            instrument_token=token,
            from_date=from_datetime,
            to_date=to_datetime,
            interval="5minute"
        )

        if candles:
            return candles[0]['volume']
        else:
            logging.warning(f"⚠️ No historical candle found for token {token} at {start_time_str}")
            return 0
    except Exception as e:
        logging.error(f" Error fetching historical volume for token {token}: {e}")
        return 0

#  Process Live OHLC Candles and Handle Nearest OTM Switching
def process_ohlc_candle():
    global current_ce_token, current_pe_token
    global last_5min_processed  # 🛠️ Important to add this

    current_time = datetime.datetime.now()
    current_minute = current_time.strftime("%Y-%m-%d %H:%M")  # "YYYY-MM-DD HH:MM"

    try:
        conn = connect_to_db()
        if not conn:
            logging.error(" Cannot connect to database inside process_ohlc_candle()")
            return

        cur = conn.cursor()

        # Step 1: Process Previous 1-Minute Candle for Both CE and PE (Only Current Tokens)
        previous_minute = (current_time - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")

        for token in [current_ce_token, current_pe_token]:
            if previous_minute in tick_buffer[token]:
                ticks = list(tick_buffer[token][previous_minute])

                if ticks:
                    prices = [tick['last_price'] for tick in ticks]
                    
                    ohlc_entry = {
                        "timestamp": previous_minute,
                        "open": prices[0],
                        "high": max(prices),
                        "low": min(prices),
                        "close": prices[-1],
                    }

                    logging.info(f" 1-min OHLC for Token {token}: {ohlc_entry}")

                # Store ticks into 5-min buffer
                tick_buffer_5min[token].extend(ticks)
                del tick_buffer[token][previous_minute]


        # Step 2: Only Process 5-min Candle and Switch Nearest OTM if NEW 5-min window
        if current_time.minute % 5 == 0:
            if last_5min_processed != current_time.strftime("%Y-%m-%d %H:%M"):
                #  New 5-min window detected
                last_5min_processed = current_time.strftime("%Y-%m-%d %H:%M")
                logging.info(f" Detected start of new 5-minute window at {current_time}")

                for token in [current_ce_token, current_pe_token]:
                    if len(tick_buffer_5min[token]) == 0:
                        continue

                    # 5-min start time
                    five_min_start = current_time - datetime.timedelta(minutes=5)
                    five_min_start = five_min_start.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

                    five_min_ticks = list(tick_buffer_5min[token])
                    five_min_prices = [tick['last_price'] for tick in five_min_ticks]
                    

                    five_min_entry = {
                        "timestamp": five_min_start,
                        "open": five_min_prices[0],
                        "high": max(five_min_prices),
                        "low": min(five_min_prices),
                        "close": five_min_prices[-1],
                        # Volume is fetched historically from the kite api as soon as 5min candle finishes
                        "volume": get_verified_5min_volume(token, five_min_start)
                    }
                    logging.info(f" 5-min OHLC for Token {token}: {five_min_entry}")

                    table_name = get_5min_table_for_token(token)

                    cur.execute(f"""
                        INSERT INTO {table_name} (timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp) DO NOTHING;
                    """, (five_min_entry["timestamp"], five_min_entry["open"], five_min_entry["high"],
                          five_min_entry["low"], five_min_entry["close"], five_min_entry["volume"]))
                    conn.commit()

                    # logging.info(f" Inserted 5-min OHLC into {table_name}")

                    # Calculate indicators
                    calculate_hl2_for_table(table_name)
                    calculate_atr_for_table(table_name)
                    calculate_initial_upper_band_for_table(table_name)
                    calculate_initial_lower_band_for_table(table_name)
                    calculate_supertrend_upper_for_table(table_name)
                    calculate_supertrend_lower_for_table(table_name)
                    calculate_oscillation_state_for_table(table_name)
                    calculate_supertrend_pivot_for_table(table_name)
                    calculate_max_channel_for_table(table_name)
                    calculate_min_channel_for_table(table_name)
                    calculate_supertrend_avg_for_table(table_name)
                    calculate_ema_for_table(table_name, length=22)
                    calculate_ema_for_table(table_name, length=33)
                    calculate_adx_for_table(table_name,period=2)

                    # logging.info(f" Indicators recalculated for {table_name}")

                    # Clear buffer
                    tick_buffer_5min[token].clear()

                # Step 3: After processing both tokens → Check nearest OTM contract switching
                update_nearest_otm_contracts()

                # Fetch latest nearest_otm_contracts from database
                conn_check = connect_to_db()
                if conn_check:
                    cur_check = conn_check.cursor()
                    cur_check.execute("""
                        SELECT ce_token, pe_token
                        FROM nearest_otm_contracts
                        ORDER BY update_timestamp DESC
                        LIMIT 1;
                    """)
                    new_result = cur_check.fetchone()

                    if new_result:
                        new_ce_token, new_pe_token = new_result

                        if new_ce_token != current_ce_token or new_pe_token != current_pe_token:
                            logging.info(" Nearest OTM Contract Changed! Switching...")

                            # Update current CE/PE tokens
                            current_ce_token = new_ce_token
                            current_pe_token = new_pe_token

                            # Create new tables
                            ce_symbol = get_symbol_from_token(current_ce_token)
                            pe_symbol = get_symbol_from_token(current_pe_token)
                            create_nearest_otm_ohlc_tables(ce_symbol, pe_symbol)

                            # Fetch historical into new tables
                            fetch_and_merge_ohlc_for_table(f"{ce_symbol.lower()}_ohlc_5min", current_ce_token, "5minute")
                            fetch_and_merge_ohlc_for_table(f"{pe_symbol.lower()}_ohlc_5min", current_pe_token, "5minute")

                            # Calculate indicators for fresh tables
                            calculate_hl2_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_atr_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_initial_upper_band_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_initial_lower_band_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_upper_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_lower_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_oscillation_state_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_pivot_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_max_channel_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_min_channel_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_avg_for_table(f"{ce_symbol.lower()}_ohlc_5min")
                            calculate_adx_for_table(f"{ce_symbol.lower()}_ohlc_5min",period=2)
                            calculate_ema_for_table(f"{ce_symbol.lower()}_ohlc_5min", length=22)
                            calculate_ema_for_table(f"{ce_symbol.lower()}_ohlc_5min", length=33)
                            

                            

                            calculate_hl2_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_atr_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_initial_upper_band_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_initial_lower_band_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_upper_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_lower_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_oscillation_state_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_pivot_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_max_channel_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_min_channel_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_supertrend_avg_for_table(f"{pe_symbol.lower()}_ohlc_5min")
                            calculate_adx_for_table(f"{pe_symbol.lower()}_ohlc_5min",period=2)
                            calculate_ema_for_table(f"{pe_symbol.lower()}_ohlc_5min", length=22)
                            calculate_ema_for_table(f"{pe_symbol.lower()}_ohlc_5min", length=33)
                            
                        

                            logging.info(" New Nearest OTM Switching completed successfully!")

                    cur_check.close()
                    conn_check.close()

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f" Error inside process_ohlc_candle(): {e}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


#  Handle WebSocket Connection
def on_connect(ws, response):
    logging.info(" WebSocket Connected. Attempting subscription...")

    try:
        time.sleep(1)  # Small delay before subscribing (prevents race condition)
        ws.subscribe(INSTRUMENT_TOKENS)
        logging.info(f" Subscription request sent for: {INSTRUMENT_TOKENS}")

        ws.set_mode(ws.MODE_FULL, INSTRUMENT_TOKENS)
        logging.info(f" Mode set to FULL for: {INSTRUMENT_TOKENS}")

        logging.info(" WebSocket is now actively listening for tick data...")

    except Exception as e:
        logging.error(f" Subscription failed: {e}")

#  Handle Incoming Tick Data & Assign to Correct Minute
def on_ticks(ws, ticks):
    for tick in ticks:
        
        token = tick['instrument_token']

        #  Small improvement: check if token belongs to the tracked INSTRUMENT_TOKENS
        if token not in INSTRUMENT_TOKENS:
            continue  # Ignore ticks for any irrelevant tokens (safety)

        tick_time = tick['exchange_timestamp'].strftime("%Y-%m-%d %H:%M")  # Extract minute part
        tick_buffer[token][tick_time].append(tick)  # Append tick to its respective minute

#  Handle WebSocket Closure & Reconnection
def on_close(ws, code, reason):
    logging.warning(f" WebSocket Closed: {code}, Reason: {reason}")
    logging.info(" Reconnecting in 5 seconds...")
    time.sleep(5)
    ws.connect(reconnect=True)

#  Handle WebSocket Errors
def on_error(ws, code, reason):
    logging.error(f" WebSocket Error Occurred! Code: {code}, Reason: {reason}")

    if "token" in reason.lower():
        logging.error(" Possible access token issue! Fetch a new one and restart.")

#  Handle Reconnection Attempts
def on_reconnect(ws, attempts):
    logging.warning(f" WebSocket Reconnecting... Attempt {attempts}")

#  Assign Event Handlers to WebSocket
kws.on_connect = on_connect
kws.on_ticks = on_ticks
kws.on_close = on_close
kws.on_error = on_error
kws.on_reconnect = on_reconnect

#  Start WebSocket After Killing Any Existing WebSocket Processes
kill_existing_websockets()
logging.info(" Starting WebSocket connection...")
kws.connect(threaded=True)

#  Process OHLC Data Every Second (Live loop)
while True:
    process_ohlc_candle()
    time.sleep(1)

