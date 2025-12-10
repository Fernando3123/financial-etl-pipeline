#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Math Validation Script
----------------------
Validates the accuracy of FinancialCalculator metrics (Beta, Sharpe, Volatility)
by comparing the class output against manual NumPy calculations.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf

# --- PATH CONFIGURATION ---
# Dynamically resolves the project root to allow importing from 'src'
# regardless of where the script is executed.
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))
# --------------------------

from src.processors.calculator import FinancialCalculator

def test_calculations():
    print("=" * 70)
    print("VALIDATING FINANCIAL MATH ENGINE")
    print("=" * 70)
    
    # 1. Setup & Data Ingestion
    print("\n[INFO] Downloading live data for validation...")
    
    # Using auto_adjust=False to maintain consistency with raw data processing
    petr = yf.download('PETR4.SA', start='2023-01-01', progress=False, auto_adjust=False)
    bvsp = yf.download('^BVSP', start='2023-01-01', progress=False, auto_adjust=False)
    
    # Robust column extraction
    petr_prices = petr['Adj Close'] if 'Adj Close' in petr.columns else petr['Close']
    bvsp_prices = bvsp['Adj Close'] if 'Adj Close' in bvsp.columns else bvsp['Close']
    
    calc = FinancialCalculator()
    
    # =========== TEST 1: VOLATILITY ===========
    print("\n" + "-" * 70)
    print("TEST 1: ANNUALIZED VOLATILITY")
    print("-" * 70)
    
    returns_petr = calc.calculate_returns(petr_prices)
    vol = calc.calculate_volatility(returns_petr, window=21)
    
    print(f"✓ Data points: {len(returns_petr)}")
    
    # Manual Verification (Last 21 days)
    last_returns = returns_petr.iloc[-21:].values
    manual_vol = np.std(last_returns, ddof=1) * np.sqrt(252)
    
    engine_vol = vol.iloc[-1]
    if isinstance(engine_vol, (pd.Series, np.ndarray)):
        engine_vol = engine_vol.item()

    print(f"> Manual: {manual_vol*100:.4f}%")
    print(f"> Engine: {engine_vol*100:.4f}%")
    
    if abs(manual_vol - engine_vol) < 0.001:
        print("✅ PASS")
    else:
        print("❌ FAIL")
    
    # =========== TEST 2: SHARPE RATIO ===========
    print("\n" + "-" * 70)
    print("TEST 2: SHARPE RATIO")
    print("-" * 70)
    
    sharpe = calc.calculate_sharpe_ratio(returns_petr, vol, risk_free_rate=0.10)
    
    # Manual Verification
    annualized_return_manual = np.mean(last_returns) * 252
    manual_sharpe = (annualized_return_manual - 0.10) / engine_vol
    
    engine_sharpe = sharpe.iloc[-1]
    if isinstance(engine_sharpe, (pd.Series, np.ndarray)):
        engine_sharpe = engine_sharpe.item()
    
    print(f"> Manual: {manual_sharpe:.4f}")
    print(f"> Engine: {engine_sharpe:.4f}")
    
    if abs(manual_sharpe - engine_sharpe) < 0.01:
        print("✅ PASS")
    else:
        print("❌ FAIL")
    
    # =========== TEST 3: BETA ===========
    print("\n" + "-" * 70)
    print("TEST 3: ROLLING BETA")
    print("-" * 70)
    
    returns_bvsp = calc.calculate_returns(bvsp_prices)
    df_aligned = pd.concat([returns_petr, returns_bvsp], axis=1).dropna()
    df_aligned.columns = ['asset', 'benchmark']
    
    beta = calc.calculate_rolling_beta(
        df_aligned['asset'],
        df_aligned['benchmark'],
        window=21
    )
    
    # Manual Verification
    last_21_asset = df_aligned['asset'].iloc[-21:].values
    last_21_bench = df_aligned['benchmark'].iloc[-21:].values
    
    cov_manual = np.cov(last_21_asset, last_21_bench)[0, 1]
    var_manual = np.var(last_21_bench, ddof=1)
    manual_beta = cov_manual / var_manual
    
    engine_beta = beta.iloc[-1]
    if isinstance(engine_beta, (pd.Series, np.ndarray)):
        engine_beta = engine_beta.item()
    
    print(f"> Manual: {manual_beta:.4f}")
    print(f"> Engine: {engine_beta:.4f}")
    
    if abs(manual_beta - engine_beta) < 0.01:
        print("✅ PASS")
    else:
        print("❌ FAIL")

if __name__ == "__main__":
    test_calculations()