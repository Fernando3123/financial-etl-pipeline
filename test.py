import pandas as pd
import sqlite3

# Load input spreadsheet
df = pd.read_excel("Base_Trades.xlsx")

# Vectorized volume calculation
df["Volume"] = df["Qtd"] * df["Preco"]

# Aggregate total volume
total_volume = df["Volume"].sum()
print(f"Total traded volume: R$ {total_volume:,.2f}")

# Save processed spreadsheet
df.to_excel("Base_Trades_Updated.xlsx", index=False)

# Initialize SQLite connection
conn = sqlite3.connect("investimentos.db")
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS trades (
    data TEXT,
    ticker TEXT,
    operacao TEXT,
    qtd INTEGER,
    preco REAL,
    volume REAL
)
""")

# Load data into SQL (replace existing table)
df.to_sql("trades", conn, if_exists="replace", index=False)

print("Data successfully stored in SQLite.")

# Sample query: filter trades by volume threshold
df_sql = pd.read_sql(
    "SELECT * FROM trades WHERE volume > 5000", 
    conn
)

print("\nSQL query result:")
print(df_sql)

# Close connection
conn.close()
