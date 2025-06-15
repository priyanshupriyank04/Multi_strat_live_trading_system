📈 Live Trend Reversal Trading System (Red-Green + EMA33 Setup)
This repository contains the code for a live options trading system based on a trend reversion strategy, using a Red-Green candle pair with EMA-33 validation and Supertrend channel as a dynamic target marker.

⚠️ Disclaimer
This project is built for educational and research purposes only. It executes real trades via the Zerodha Kite API.
No profitability or performance guarantees are made. Use this system entirely at your own risk.

🛠 Requirements
Zerodha Kite API key and secret

PostgreSQL instance with nearest_otm_contracts and OHLC data tables for CE/PE contracts

Python 3.8+

Required dependencies listed in requirements.txt

📁 Code Structure
File	Description
s1_main.py	Connects to WebSocket to receive live tick data and updates real-time CE/PE LTP values.
s1_execute.py	Checks for trade alerts on each new 5-minute candle. Executes CE/PE trades based on Red-Green + EMA-33 setup. Manages entries, SL/target, and exits in real-time.

🧠 Strategy Summary
Trend Reversal Basis:

Looks for a Red-Green candle pair (previous candle red, current candle green).

Ensures 33 EMA is rising over the 3 candles before the red-green pair.

Trigger Logic:

If an alert is found, the system watches for a live price breakout above the green candle high within 5 minutes.

Entry is executed using a market order when the breakout occurs.

Stop Loss: Green candle's low

Target: Supertrend Upper Band (stored as max_channel in DB)

🔄 Current Limitations
The execution logic currently handles only one strategy (Red-Green + EMA-33 on the 5-minute timeframe).

It works on nearest OTM CE and PE contracts selected dynamically based on the NIFTY index price.

💡 What's Next
To evolve this into a multi-strategy live trading system, you are encouraged to:

Add support for ADX and CBOE-based indicators as separate strategies

Use a modular strategy checker where each strategy runs independently and in parallel

Maintain strategy metadata to track performance, trades, and active signals per strategy

