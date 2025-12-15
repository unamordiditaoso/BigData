import pandas as pd
import requests
from pathlib import Path
import os
import sqlite3

DATA_CSV = "../output/paisesProcesados.csv"
OUTPUT_CSV = "../output/paisesFinal.csv"
LOG_PATH = "../logs/pipeline.log"
IMAGES_DIR = Path("../output/imagenes_procesadas")

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

def log_message(msg):
    with open(LOG_PATH, "a", encoding="utf-8") as log:
        log.write(msg + "\n")
    print(msg)

def get_local_flag_path(flag_url):
    img_stem = Path(flag_url).stem  # 'af' de 'https://flagcdn.com/w320/af.png'
    local_path = IMAGES_DIR / f"{img_stem}.jpg"  # convertir a .jpg local
    if local_path.exists():
        return str(local_path)
    return None

try:
    df = pd.read_csv(DATA_CSV)
    log_message(f"[OK] CSV leído correctamente: {df.shape[0]} filas, {df.shape[1]} columnas")
except Exception as e:
    log_message(f"[ERROR] Leyendo CSV: {e}")
    raise

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(".", "_")

if "account_t_d" in df.columns:
    df["account_t_d"] = pd.to_numeric(df["account_t_d"].astype(str).str.rstrip('%'), errors="coerce")
    df["account_t_d"] = df["account_t_d"].round(2)

try:
    url = "https://restcountries.com/v2/all?fields=alpha2Code,name,capital,region,subregion,flags"
    response = requests.get(url)
    response.raise_for_status()
    countries_api = pd.json_normalize(response.json())
    log_message(f"[OK] API REST Countries obtenida: {countries_api.shape[0]} filas")
except Exception as e:
    log_message(f"[ERROR] Llamando a API: {e}")
    raise

countries_api = countries_api[['alpha2Code', 'name', 'capital', 'region', 'subregion', 'flags.png']]
countries_api.rename(columns={
    'alpha2Code': 'codewb',
    'name': 'country_name_api',
    'capital': 'capital_api',
    'region': 'region_api',
    'subregion': 'subregion_api',
    'flags.png': 'flag_url'
}, inplace=True)

if "countrynewwb" not in df.columns:
    log_message("[ERROR] No existe columna 'countrynewwb' en CSV para hacer join")
    raise KeyError("countrynewwb missing in CSV")

df_final = pd.merge(
    df,
    countries_api,
    left_on='countrynewwb',
    right_on='country_name_api',
    how='left'
)

df_final['countrynewwb_norm'] = df_final['countrynewwb'].astype(str).str.strip().str.lower()
countries_api['country_name_api_norm'] = countries_api['country_name_api'].astype(str).str.strip().str.lower()

missing = df_final[df_final['country_name_api'].isnull()].copy()

for idx, row in missing.iterrows():
    name_to_search = row['countrynewwb_norm']
    matches = countries_api[countries_api['country_name_api_norm'].str.contains(name_to_search, na=False, regex=False)]
    
    if not matches.empty:
        df_final.loc[idx, 'country_name_api'] = matches.iloc[0]['country_name_api']
        df_final.loc[idx, 'capital_api'] = matches.iloc[0]['capital_api']
        df_final.loc[idx, 'region_api'] = matches.iloc[0]['region_api']
        df_final.loc[idx, 'subregion_api'] = matches.iloc[0]['subregion_api']
        df_final.loc[idx, 'flag_url'] = matches.iloc[0]['flag_url']

df_final = df_final[df_final['country_name_api'].notnull()].copy()

missing_api = df_final[df_final['country_name_api'].isnull()]
log_message(f"[INFO] Países sin información en API: {missing_api.shape[0]}")

df_final.rename(columns={'codewb_x': 'codewb'}, inplace=True)
df_final.drop_duplicates(subset=["countrynewwb", "year"], inplace=True)
df_final.drop(columns=['countrynewwb_norm', 'codewb_y', 'country_name_api'], inplace=True)

# IMAGENES DE LAS BANDERAS

df_final.loc[df_final['countrynewwb'] == "Afghanistan", 'flag_url'] = "https://flagcdn.com/w320/af.png"

df_final['local_flag_path'] = df_final['flag_url'].apply(get_local_flag_path)

df_final = df_final[df_final['local_flag_path'].notnull()].copy()

df_final.to_csv(OUTPUT_CSV, index=False)
log_message(f"[OK] CSV final guardado: {OUTPUT_CSV}")

DB_PATH = Path("../data/bd/paises.db")  # tu archivo SQLite
TABLE_NAME = "countries_data"

# Conexión
conn = sqlite3.connect(DB_PATH)

# Cargar DataFrame en SQLite
df_final.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

log_message(f"[OK] Datos cargados correctamente en {DB_PATH} en la tabla '{TABLE_NAME}'")

# Cerrar conexión
conn.close()
