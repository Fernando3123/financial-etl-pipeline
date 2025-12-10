# src/processors/calculator.py
import pandas as pd
import numpy as np

class FinancialCalculator:
    """
    Processors Layer — Financial metrics computation module.
    """

    def calculate_returns(self, prices: pd.Series) -> pd.Series:
        """
        Compute simple daily returns.
        Formula: return_t = price_t / price_(t-1) - 1
        """
        return prices.pct_change().dropna()

    def calculate_volatility(self, returns: pd.Series, window: int = 21) -> pd.Series:
        """
        Compute annualized rolling volatility.
        Formula: vol_t = rolling_std(returns, window) * sqrt(252)
        """
        vol = returns.rolling(window=window).std() * np.sqrt(252)
        return vol.dropna()

    def calculate_sharpe_ratio(self, returns: pd.Series, volatility_annualized: pd.Series, risk_free_rate=0.10) -> pd.Series:
        """
        Computes Rolling Sharpe Ratio.
        Assumption: Risk Free Rate is constant (e.g., 10% p.a.) for simplicity.
        Formula: (Annualized Return - RiskFree) / Volatility
        """
        # Annualize the daily return (approximate: mean daily return * 252)
        annualized_return = returns.rolling(window=21).mean() * 252
        
        sharpe = (annualized_return - risk_free_rate) / volatility_annualized
        return sharpe

    def calculate_rolling_beta(self, asset_returns: pd.Series, benchmark_returns: pd.Series, window=21) -> pd.Series:
        """
        Computes Rolling Beta (Sensitivity to Market).
        Formula: Covariance(Asset, Market) / Variance(Market)
        """
        # We need to align indexes perfectly between asset and benchmark
        # join='inner' ensures we only calculate when both exist
        df_join = pd.concat([asset_returns, benchmark_returns], axis=1, join='inner')
        df_join.columns = ['asset', 'market']
        
        # Calculate Rolling Covariance and Variance
        rolling_cov = df_join['asset'].rolling(window).cov(df_join['market'])
        rolling_var = df_join['market'].rolling(window).var()
        
        beta = rolling_cov / rolling_var
        return beta

    def calculate_cumulative_return(self, returns: pd.Series) -> pd.Series:
        """
        Compute cumulative return (growth of a R$1.00 investment).
        """
        return (1 + returns).cumprod() - 1