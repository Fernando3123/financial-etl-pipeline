import pandas as pd

# Read input spreadsheet
df = pd.read_excel("Base_Trades.xlsx")

# Vectorized calculation of traded volume
df["Volume"] = df["Qtd"] * df["Preco"]

# Aggregate result
total_volume = df["Volume"].sum()
print(f"Total traded volume: R$ {total_volume:,.2f}")

# Show updated table (optional)
print("\nUpdated data:")
print(df)

# Save processed file
df.to_excel("Base_Trades_Updated.xlsx", index=False)
print("File saved successfully.")
