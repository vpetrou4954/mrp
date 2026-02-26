import pandas as pd

# Διαβάζει το αρχείο receipes.xlsx και εμφανίζει τις στήλες
file_path = "receipes.xlsx"
df = pd.read_excel(file_path)
print("Στήλες αρχείου συνταγών:")
for col in df.columns:
    print(col)
