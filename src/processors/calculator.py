import pandas as pd
import numpy as np

class FinancialCalculator:
    """
    Processors Layer — Financial metrics computation module.
    """

    def calculate_returns(self, df_prices: pd.DataFrame) -> pd.DataFrame:
        """
        Compute simple daily returns.
        
        Formula:
            return_t = price_t / price_(t-1) - 1
        
        Notes:
        - pct_change() performs the calculation efficiently.
        - dropna() removes the first row (no previous day to compare).
        """
        return df_prices.pct_change().dropna()

    def calculate_volatility(self, df_returns: pd.DataFrame, window: int = 21) -> pd.DataFrame:
        """
        Compute annualized rolling volatility.

        Parameters:
            df_returns : Daily returns.
            window     : Rolling window size (default: 21 trading days ≈ 1 month).

        Annualization factor:
            sqrt(252) — number of trading days in a year.

        Formula:
            vol_t = rolling_std(df_returns, window) * sqrt(252)
        """
        vol = df_returns.rolling(window=window).std() * np.sqrt(252)
        return vol.dropna()

    def calculate_cumulative_return(self, df_returns: pd.DataFrame) -> pd.DataFrame:
        """
        Compute cumulative return (growth of a R$1.00 investment).

        Formula:
            cumulative_t = (1 + r1) * (1 + r2) * ... * (1 + rt) - 1
        """
        return (1 + df_returns).cumprod() - 1
