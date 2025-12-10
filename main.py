# main.py
import pandas as pd
import yaml # Importamos a biblioteca nova
import sys
from pathlib import Path

# Adiciona imports do projeto
from src.data_providers.yahoo_provider import YahooMarketData
from src.processors.calculator import FinancialCalculator
from src.data_providers.repository import RiskDatabase

def load_config():
    """Carrega as configurações do arquivo YAML."""
    config_path = Path(__file__).parent / "config.yaml"
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            print("[CONFIG] Arquivo config.yaml carregado com sucesso.")
            return config
    except FileNotFoundError:
        print("[CRITICAL] Arquivo config.yaml não encontrado!")
        sys.exit(1)

def run_pipeline():
    print("[INFO] Initializing Financial Risk Engine...")
    
    # 1. Configuration & Setup (AGORA DINÂMICO)
    config = load_config()
    
    # Extraindo dados do YAML
    tickers_list = config['assets']['tickers']
    benchmark_ticker = config['assets']['benchmark']
    period_config = config['pipeline']['period']
    risk_free = config['pipeline']['risk_free_rate']
    
    # Garantir que o benchmark esteja na lista de download
    full_tickers = tickers_list + [benchmark_ticker]
    # Remove duplicatas caso o benchmark já esteja na lista
    full_tickers = list(set(full_tickers))
    
    # 2. Ingestion Layer
    print(f"[INFO] Fetching historical data ({period_config}) for {len(full_tickers)} assets...")
    market_data = YahooMarketData(full_tickers)
    raw_prices = market_data.get_history(period=period_config)
    
    # 3. Processing Layer
    print("[INFO] Processing risk metrics (Returns, Volatility, Sharpe, Beta)...")
    calculator = FinancialCalculator()
    repository = RiskDatabase()
    
    # PRE-CALCULATION: Prepare Benchmark Data
    benchmark_sa = f"{benchmark_ticker}.SA"
    if benchmark_sa not in raw_prices.columns:
        print(f"[CRITICAL ERROR] Benchmark {benchmark_sa} not found. Cannot calculate Beta.")
        return

    s_benchmark_prices = raw_prices[benchmark_sa]
    s_benchmark_returns = calculator.calculate_returns(s_benchmark_prices)

    processed_frames = []

    # Iteramos apenas sobre a lista de ativos definida no YAML (excluindo benchmark se não for alvo)
    for ticker in tickers_list:
        ticker_sa = f"{ticker}.SA"
        
        if ticker_sa in raw_prices.columns:
            # Extract Series
            s_price = raw_prices[ticker_sa]
            
            # --- CORE METRICS ---
            s_return = calculator.calculate_returns(s_price)
            s_vol = calculator.calculate_volatility(s_return)
            
            # --- ADVANCED METRICS ---
            # Passamos a risk_free que veio do YAML
            s_sharpe = calculator.calculate_sharpe_ratio(s_return, s_vol, risk_free_rate=risk_free)
            s_beta = calculator.calculate_rolling_beta(s_return, s_benchmark_returns)
            
            # --- SUPREME FUSION ---
            df_ticker = pd.concat([s_price, s_return, s_vol, s_sharpe, s_beta], axis=1, join='inner')
            
            df_ticker.columns = ['close', 'daily_return', 'volatility_21d', 'sharpe_ratio', 'beta_21d']
            df_ticker['ticker'] = ticker
            df_ticker['date'] = df_ticker.index 
            
            processed_frames.append(df_ticker)
        else:
            print(f"[WARN] No data found for {ticker}. Skipping.")

    # 4. Persistence Layer
    if processed_frames:
        final_df = pd.concat(processed_frames)
        final_df['date'] = pd.to_datetime(final_df['date']).dt.date
        
        print("[INFO] Persisting data to SQLite database...")
        repository.save_data(final_df)
        
        print(f"[SUCCESS] Pipeline executed. Processed {len(processed_frames)} assets.")
    else:
        print("[ERROR] Pipeline failed to process any data.")

if __name__ == "__main__":
    run_pipeline()