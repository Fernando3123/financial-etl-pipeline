# src/data_providers/repository.py
import sqlite3
import pandas as pd
from pathlib import Path

class RiskDatabase:
    """
    Manages data persistence using SQLite.
    Pattern: Repository.
    """
    
    def __init__(self, db_name="financial_risk.db"):
        # Resolves path: 'data' folder at the project root level
        root_path = Path(__file__).parent.parent.parent
        self.db_path = root_path / "data" / db_name
        
        # Ensures the 'data' directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initializes the database schema if it does not exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Defining 'market_data' schema
        # UPDATED: Added sharpe_ratio and beta_21d columns
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                date DATE,
                ticker TEXT,
                close REAL,
                daily_return REAL,
                volatility_21d REAL,
                sharpe_ratio REAL,
                beta_21d REAL
            )
        """)
        conn.commit()
        conn.close()

    def save_data(self, data: pd.DataFrame):
        """
        Persists the processed DataFrame into the database.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            data.to_sql("market_data", conn, if_exists="replace", index=False)
            print(f"[SUCCESS] Data successfully saved to: {self.db_path}")
        except Exception as e:
            print(f"[ERROR] Failed to save to database: {e}")
        finally:
            conn.close()