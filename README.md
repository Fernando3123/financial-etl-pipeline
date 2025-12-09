# 📈 Financial Data ETL Pipeline

**Status:** 🚧 Phase 1 — Initial MVP

## 📋 Overview
This project is the foundation of a Portfolio Monitoring System.  
The first version implements a simple **ETL pipeline** to process trade data using Python and Pandas, replacing manual Excel workflows.

## 🛠️ Tech Stack
- **Language:** Python 3.12+
- **Libraries:** Pandas, OpenPyXL
- **Data Source:** Excel (.xlsx)

## 🚀 How It Works (Phase 1)
The script performs the following steps:

1. **Extract**  
   Loads raw trade data from an Excel file into a DataFrame.

2. **Transform**  
   Uses vectorized operations to compute traded volume (`Qtd * Preco`) and aggregate total results.

3. **Load**  
   Exports the processed dataset to a new Excel file optimized for Power BI.

## 🔮 Roadmap (Next Steps)
This repository documents my progression in Data & Financial Engineering. Future additions include:

- [ ] Migration to SQL Database (SQLite/PostgreSQL)  
- [ ] Add financial risk metrics (Volatility, VaR, Beta)  
- [ ] Integration with Power BI dashboards  
- [ ] Automate data retrieval from B3 / Yahoo Finance APIs  

---

*Created by Fernando — CEA Certified*
