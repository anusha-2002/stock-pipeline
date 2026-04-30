import yfinance as yf
import pandas as pd
import os

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
START_DATE = "2020-01-01"
END_DATE = "2024-12-31"

output_dir = os.path.dirname(os.path.abspath(__file__))

for symbol in SYMBOLS:
    df = yf.download(symbol, start=START_DATE, end=END_DATE, auto_adjust=True)
    df["symbol"] = symbol
    df.reset_index(inplace=True)
    output_path = os.path.join(output_dir, f"{symbol}.csv")
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows for {symbol}")

print("All downloads complete.")
