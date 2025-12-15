import sqlite3
import pandas as pd
import cv2
from pathlib import Path
from tabulate import tabulate

DB_PATH = Path("../data/bd/paises.db")
TABLE_NAME = "countries_data"

conn = sqlite3.connect(DB_PATH)

country_input = input("Escribe el nombre del país: ").strip().lower()

query = f"""
SELECT * FROM {TABLE_NAME}
WHERE LOWER(countrynewwb) = ?
"""
df_country = pd.read_sql(query, conn, params=(country_input,))

conn.close()

if df_country.empty:
    print(f"No se encontró información para '{country_input}'.")
else:
    rename_cols = {
        "countrynewwb": "País",
        "codewb": "Código WB",
        "year": "Año",
        "pop_adult": "Población Adultos",
        "regionwb24_hi": "Región WB",
        "incomegroupwb24": "Grupo Ingreso",
        "account_t_d": "Cuenta de Banco: 15+",
        "capital_api": "Capital",
        "region_api": "Region",
        "subregion_api": "Subregion"
    }
    cols_to_show = [c for c in df_country.columns if c not in ["local_flag_path", "flag_url"]]
    df_display = df_country[cols_to_show].rename(columns=rename_cols)
    df_display['Población Adultos'] = df_display['Población Adultos'].apply(lambda x: f"{x:,.0f}".replace("," , "."))

    print("\nInformación del país:")
    #print(df_display.to_string(index=False, col_space=15))
    print(tabulate(df_display, headers='keys', tablefmt='grid', showindex=False))

    flag_path = df_country.iloc[0]["local_flag_path"]
    if flag_path and Path(flag_path).exists():
        img = cv2.imread(str(flag_path))
        cv2.namedWindow(f"Bandera de {country_input.title()}", cv2.WINDOW_NORMAL)
        cv2.setWindowProperty(
            f"Bandera de {country_input.title()}", 
            cv2.WND_PROP_TOPMOST, 
            1
        )
        cv2.imshow(f"Bandera de {country_input.title()}", img)
        cv2.waitKey(0)
        cv2.destroyAllWindows()
    else:
        print("No se encontró la bandera local para este país.")
