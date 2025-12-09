# src/data_providers/yahoo_provider.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class YahooMarketData:
    """
    Responsible for fetching historical market data via Yahoo Finance.
    Design Pattern: Data Provider.
    """
    
    def __init__(self, tickers: list):
        # Normalizes tickers to B3 format (ensures .SA suffix)
        self.tickers = [t if t.endswith(".SA") else f"{t}.SA" for t in tickers]
    
    def get_history(self, period="2y") -> pd.DataFrame:
        """
        Retrieves adjusted closing prices (Adj Close).
        Returns a clean DataFrame with Date index.
        """
        print(f"[INFO] Fetching data for: {self.tickers}...")
        
        try:
            # Batch download for performance optimization
            data = yf.download(
                self.tickers, 
                period=period, 
                group_by='ticker', 
                auto_adjust=True,
                progress=False
            )
            
            # Extracting specific 'Close' column from the MultiIndex structure
            df_close = pd.DataFrame()
            
            for t in self.tickers:
                try:
                    df_close[t] = data[t]['Close']
                except KeyError:
                    print(f"[WARN] Failed to process ticker {t}. It might not exist.")
            
            # Cleaning empty rows (e.g., holidays)
            df_close.dropna(how='all', inplace=True)
            
            print("[SUCCESS] Market data retrieved successfully.")
            return df_close

        except Exception as e:
            print(f"[ERROR] Critical failure during download: {e}")
            return pd.DataFrame()