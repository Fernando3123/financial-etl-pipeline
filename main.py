# main.py
import pandas as pd
from src.data_providers.yahoo_provider import YahooMarketData
from src.processors.calculator import FinancialCalculator
from src.data_providers.repository import RiskDatabase

def run_pipeline():
    """
    Orchestrates the Financial Risk Engine pipeline:
    1. Ingestion (Yahoo Finance)
    2. Processing (Risk Metrics Calculation)
    3. Persistence (SQLite Database)
    """
    print("[INFO] Initializing Financial Risk Engine...")
    
    # 1. Configuration & Setup
    # Tickers list (Brazilian assets will be handled by the provider)
    tickers = ["PETR4", "VALE3", "ITUB4", "BOVA11", "WEGE3", "AAPL34"]
    
    # 2. Ingestion Layer
    print(f"[INFO] Fetching historical data for {len(tickers)} assets...")
    market_data = YahooMarketData(tickers)
    raw_prices = market_data.get_history(period="2y")
    
    # 3. Processing Layer
    print("[INFO] Processing risk metrics (Returns & Volatility)...")
    calculator = FinancialCalculator()
    repository = RiskDatabase()
    
    processed_frames = []

    for ticker in tickers:
        ticker_sa = f"{ticker}.SA"
        
        # Validate data availability for the ticker
        if ticker_sa in raw_prices.columns:
            # Extract Series
            s_price = raw_prices[ticker_sa]
            
            # Compute Metrics
            s_return = calculator.calculate_returns(s_price)
            s_vol = calculator.calculate_volatility(s_return)
            
            # Data Alignment (Inner Join)
            # Ensures data integrity by dropping rows with missing metrics (e.g., NaN volatility)
            df_ticker = pd.concat([s_price, s_return, s_vol], axis=1, join='inner')
            
            # Standardization for Database Schema
            df_ticker.columns = ['close', 'daily_return', 'volatility_21d']
            
            # Metadata injection
            df_ticker['ticker'] = ticker
            df_ticker['date'] = df_ticker.index 
            
            processed_frames.append(df_ticker)
        else:
            print(f"[WARN] No data found for {ticker}. Skipping.")

    # 4. Persistence Layer
    if processed_frames:
        # Consolidate all assets into a single normalized table
        final_df = pd.concat(processed_frames)
        
        # Type enforcement for SQL compatibility
        final_df['date'] = pd.to_datetime(final_df['date']).dt.date
        
        print("[INFO] Data processing complete. Sample output:")
        print(final_df.tail())

        print("[INFO] Persisting data to SQLite database...")
        repository.save_data(final_df)
        
        print("[SUCCESS] Pipeline executed successfully.")
    else:
        print("[ERROR] Pipeline failed to process any data.")

if __name__ == "__main__":
    run_pipeline()