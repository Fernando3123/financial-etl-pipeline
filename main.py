# main.py
import pandas as pd
from src.data_providers.yahoo_provider import YahooMarketData
from src.processors.calculator import FinancialCalculator
from src.data_providers.repository import RiskDatabase

def run_pipeline():
    """
    Orchestrates the Financial Risk Engine pipeline:
    1. Ingestion (Yahoo Finance)
    2. Processing (Risk Metrics: Volatility, Sharpe, Beta)
    3. Persistence (SQLite Database)
    """
    print("[INFO] Initializing Financial Risk Engine...")
    
    # 1. Configuration & Setup
    # BOVA11 is required as the benchmark for Beta calculation
    tickers = ["PETR4", "VALE3", "ITUB4", "BOVA11", "WEGE3", "AAPL34"]
    benchmark_ticker = "BOVA11" 
    
    # 2. Ingestion Layer
    print(f"[INFO] Fetching historical data for {len(tickers)} assets...")
    market_data = YahooMarketData(tickers)
    raw_prices = market_data.get_history(period="2y")
    
    # 3. Processing Layer
    print("[INFO] Processing risk metrics (Returns, Volatility, Sharpe, Beta)...")
    calculator = FinancialCalculator()
    repository = RiskDatabase()
    
    # PRE-CALCULATION: Prepare Benchmark Data (BOVA11)
    benchmark_sa = f"{benchmark_ticker}.SA"
    if benchmark_sa not in raw_prices.columns:
        print(f"[CRITICAL ERROR] Benchmark {benchmark_sa} not found. Cannot calculate Beta.")
        return

    s_benchmark_prices = raw_prices[benchmark_sa]
    s_benchmark_returns = calculator.calculate_returns(s_benchmark_prices)

    processed_frames = []

    for ticker in tickers:
        ticker_sa = f"{ticker}.SA"
        
        if ticker_sa in raw_prices.columns:
            # Extract Series
            s_price = raw_prices[ticker_sa]
            
            # --- CORE METRICS ---
            s_return = calculator.calculate_returns(s_price)
            s_vol = calculator.calculate_volatility(s_return)
            
            # --- ADVANCED METRICS ---
            s_sharpe = calculator.calculate_sharpe_ratio(s_return, s_vol)
            s_beta = calculator.calculate_rolling_beta(s_return, s_benchmark_returns)
            
            # --- SUPREME FUSION (Data Alignment) ---
            # Inner join ensures all metrics are available for the row
            df_ticker = pd.concat([s_price, s_return, s_vol, s_sharpe, s_beta], axis=1, join='inner')
            
            # Standardization for Database Schema
            df_ticker.columns = ['close', 'daily_return', 'volatility_21d', 'sharpe_ratio', 'beta_21d']
            
            # Metadata injection
            df_ticker['ticker'] = ticker
            df_ticker['date'] = df_ticker.index 
            
            processed_frames.append(df_ticker)
        else:
            print(f"[WARN] No data found for {ticker}. Skipping.")

    # 4. Persistence Layer
    if processed_frames:
        final_df = pd.concat(processed_frames)
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