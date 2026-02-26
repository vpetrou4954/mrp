import pandas as pd

# Διαβάζει το αρχείο sales.xlsx και εμφανίζει τις στήλες
file_path = "sales.xlsx"
df = pd.read_excel(file_path)
print("Στήλες αρχείου παραγγελιών:")
for col in df.columns:
    print(col)
