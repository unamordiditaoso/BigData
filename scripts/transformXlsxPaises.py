import pandas as pd
import os

DATA_PATH = "../data/GlobalFindexDatabase2025.xlsx"
OUTPUT_PATH = "../output/paisesProcesados.csv"
LOG_PATH = "../logs/extract_transform_validate.log"

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

try:
    df = pd.read_excel(DATA_PATH, sheet_name="Data",skiprows=[1])
    with open(LOG_PATH, "a") as log:
        log.write(f"Excel leido correctamente, filas: {df.shape[0]}, columnas: {df.shape[1]}\n")
except Exception as e:
    with open(LOG_PATH, "a") as log:
        log.write(f"Error leyendo Excel: {e}\n")
    raise


columns_needed = ["countrynewwb", "codewb", "year", "pop_adult", "regionwb24_hi", "incomegroupwb24", "account.t.d"]
df = df[columns_needed]

df.columns = [col.lower().replace(" ", "_") for col in df.columns]

df["pop_adult"] = pd.to_numeric(df["pop_adult"], errors="coerce")
df["account.t.d"] = df["account.t.d"].astype(str)
df["account.t.d"] = df["account.t.d"].str.rstrip('%').astype(float)
df["account.t.d"] = df["account.t.d"].round(2)
df["year"] = pd.to_numeric(df["year"], errors="coerce")

missing_values = df.isnull().sum()
with open(LOG_PATH, "a") as log:
    log.write(f"Valores faltantes por columna:\n{missing_values}\n")

duplicates = df.duplicated(subset=["codewb", "year"]).sum()
with open(LOG_PATH, "a") as log:
    log.write(f"Filas duplicadas por codewb + year: {duplicates}\n")

invalid_rows = df[(df["pop_adult"] <= 0)]
with open(LOG_PATH, "a") as log:
    log.write(f"Filas con valores invalidos (populacion <= 0): {invalid_rows.shape[0]}\n")

df.to_csv(OUTPUT_PATH, index=False)
with open(LOG_PATH, "a") as log:
    log.write(f"Archivo limpio guardado en: {OUTPUT_PATH}\n")