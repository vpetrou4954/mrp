import pandas as pd
import re

# Διαβάζει το αρχείο συνταγών
file_path = "receipes.xlsx"
df = pd.read_excel(file_path)

# Φιλτράρει μόνο τις ενεργές γραμμές
active_df = df[df["Ανενεργό"] == 0]

# Ζητάει είδος και ποσότητα από τον χρήστη
product_code = input("Δώσε Κωδικό Είδους Συνταγής: ").strip()
qty = float(input("Δώσε ποσότητα παραγωγής: "))

# Βρίσκει όλες τις συνταγές για το είδος
recipes = active_df[active_df["Κωδικός Είδους Συνταγής"] == product_code]

if recipes.empty:
    print("Δεν βρέθηκε συνταγή για τον κωδικό.")
    exit()

# Επιλογή συνταγής με μεγαλύτερη έκδοση (π.χ. 121-00-75/4)
def get_revision_number(code):
    match = re.search(r"/(\d+)$", str(code))
    return int(match.group(1)) if match else 0

recipes["rev_num"] = recipes["Κωδικός"].apply(get_revision_number)
max_rev = recipes["rev_num"].max()
selected = recipes[recipes["rev_num"] == max_rev]

# Παίρνει την ποσότητα παραγόμενου από τη συνταγή
base_qty = selected["Ποσότητα Παραγόμενου"].iloc[0]

print(f"\nΥλικά για παραγωγή {qty} τεμ. του είδους {product_code} (συνταγή έκδοσης /{max_rev}):\n")

for idx, row in selected.iterrows():
    needed = qty / base_qty * row["Ποσότητα"]
    print(f"{row['Κωδ. Αναλ.']} - {row['Περιγραφή Αναλούμενου']}: {needed:.3f} {row['Μονάδα μέτρησης']}")
