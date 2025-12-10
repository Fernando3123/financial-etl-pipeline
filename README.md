# Financial Risk Monitoring Engine

A modular quantitative pipeline for ingesting market data, computing core risk metrics, and storing normalized time-series into a SQL repository for analytics and BI consumption.

This project models the architecture found in real-world financial data engineering pipelines — **ingest → transform → compute → persist** — following professional software design principles.

## 🎯 Project Overview
This repository implements a small-scale Risk Engine capable of:
- Fetching historical adjusted prices from market data providers
- Computing essential risk indicators
- Aligning time-series to ensure mathematical consistency
- Persisting normalized data into a relational database

It is designed as a portfolio-grade project demonstrating skills required in quantitative finance, data engineering, and Python-based financial analytics.

## 📦 Scope & Features
- **Automated Data Ingestion:** Historical price retrieval using Yahoo Finance.
- **Normalization Pipeline:**
  - Wide → Long formatting
  - Strict index alignment (implicit inner-join via time-based filtering)
- **Risk Metric Computation:**
  - Daily returns
  - 21-day annualized volatility
  - Cumulative return (optional future step)
- **Persistence Layer:**
  - SQLite repository pattern
  - Append/replace modes
  - Structured schema for multi-asset storage
- **BI-Ready Output:**
  - SQL table optimized for Power BI or Python analytics

## 🛠 Tech Stack
- **Python 3.14+**
- **Pandas, NumPy**
- **YFinance** (API wrapper)
- **SQLite3** (native database engine)
- **pyproject.toml** (PEP-621 compliant)
- **Power BI / Jupyter** (consumption layer)

## 🧠 Demonstrated Competencies
- Handling financial time-series with market conventions (e.g., .SA tickers)
- **ETL design with clear separation of concerns:**
  - Data Providers
  - Processors (risk logic)
  - Repository layer (SQL)
- OOP architecture for pipelines
- Time-series validation and alignment
- SQL relational modeling for analytics workloads

## 📁 Repository Structure
```text
financial-monitoring-engine/
├── data/                   # SQLite database storage
├── src/
│   ├── data_providers/     # Market data ingestion & SQL repository
│   ├── processors/         # Risk & statistical calculations
├── tests/                  # (future) unit tests
├── main.py                 # Pipeline orchestrator
└── pyproject.toml          # Build metadata