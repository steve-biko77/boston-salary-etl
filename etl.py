
import json
import urllib.request
import pandas as pd
import numpy as np
import re

def extract_boston_salary(url: str, limit: int = 1000) -> pd.DataFrame:
    """Extrait les données brutes depuis l'API Boston avec pagination."""
    all_records = []
    offset = 0
    
    while True:
        paginated_url = f"{url}&limit={limit}&offset={offset}"
        try:
            with urllib.request.urlopen(paginated_url) as response:
                data = json.loads(response.read().decode())
                records = data['result']['records']
                all_records.extend(records)
                if len(records) < limit:
                    break
                offset += limit
        except Exception as e:
            print(f"Erreur lors de la récupération des données : {e}")
            break
    
    df = pd.DataFrame(all_records)
    print(f"Nombre de lignes extraites : {len(df)}")
    print(f"Colonnes extraites : {df.columns.tolist()}")
    return df

def clean_numeric_string(s: str) -> str:
    """Nettoie une chaîne numérique pour la rendre convertible en float."""
    if pd.isna(s) or s in ["None", "", " ", None]:
        return np.nan
    s = str(s).strip()
    s = s.replace(" ", "").replace(",", ".")
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    return s

def transform_boston_salary(df: pd.DataFrame) -> pd.DataFrame:
    """Transforme les données de salaire de Boston."""
    rename_map = {
        "RÉGULIER": "REGULAR",
        "REGULAR": "REGULAR",
        "RÉTRO": "RETRO",
        "RETRO": "RETRO",
        "AUTRE": "OTHER",
        "OTHER": "OTHER",
        "HEURES SUPPLÉMENTAIRES": "OVERTIME",
        "OVERTIME": "OVERTIME",
        "BLESSÉ": "INJURED",
        "INJURED": "INJURED",
        "DÉTAIL": "DETAIL",
        "DETAIL": "DETAIL",
        "QUINN/ÉDUCATION INCITATION": "EDUCATION_INCENTIVE",
        "QUINN/EDUCATION INCENTIVE": "EDUCATION_INCENTIVE",
        "INCITATION À L'ÉDUCATION/QUINN": "EDUCATION_INCENTIVE",
        "QUINN/INCITATION À L'ÉDUCATION": "EDUCATION_INCENTIVE",
        "INCITATION QUINN/ÉDUCATION": "EDUCATION_INCENTIVE",
        "GAINS TOTAUX": "TOTAL_EARNINGS",
        "TOTAL GAINS": "TOTAL_EARNINGS",
        "TOTAL DES GAIN": "TOTAL_EARNINGS",
        "TOTAL DES GAINS": "TOTAL_EARNINGS",
        "TOTAL EARNINGS": "TOTAL_EARNINGS"
    }
    
    unrecognized_cols = [col for col in df.columns if col not in rename_map and col not in ["_id", "NAME", "DEPARTMENT_NAME", "TITLE", "POSTAL"]]
    if unrecognized_cols:
        print(f"Colonnes non reconnues dans rename_map : {unrecognized_cols}")
    
    existing_columns = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_columns)
    
    numeric_cols = [
        "REGULAR", "RETRO", "OTHER", "OVERTIME", 
        "INJURED", "DETAIL", "EDUCATION_INCENTIVE", "TOTAL_EARNINGS"
    ]
    numeric_cols = [col for col in numeric_cols if col in df.columns]
    
    for col in numeric_cols:
        df[col] = df[col].apply(clean_numeric_string)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    component_cols = [col for col in numeric_cols if col != "TOTAL_EARNINGS"]
    if component_cols:
        df["SUM_COMPONENTS"] = df[component_cols].sum(axis=1)
    else:
        df["SUM_COMPONENTS"] = 0
    
    if "TOTAL_EARNINGS" not in df.columns:
        print("Avertissement : La colonne 'TOTAL_EARNINGS' n'est pas présente après transformation.")
    
    print(f"Colonnes après transformation : {df.columns.tolist()}")
    return df

def load(df: pd.DataFrame, filename: str = "boston_salaries_clean.csv") -> None:
    """Enregistre les données nettoyées dans un fichier CSV."""
    try:
        df.to_csv(filename, index=False)
        print(f"Données enregistrées avec succès dans {filename}")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement du fichier CSV : {e}")

def analyse(df: pd.DataFrame) -> dict:
    """Réalise des calculs statistiques sur les salaires par département."""
    required_cols = ["DEPARTMENT_NAME", "TOTAL_EARNINGS"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Erreur : Les colonnes {missing_cols} sont manquantes.")
        return {}
    
    stats = df.groupby("DEPARTMENT_NAME")["TOTAL_EARNINGS"].agg(
        mean="mean",
        median="median",
        min="min",
        max="max",
        std="std",
        count="count"
    ).round(2)
    
    result = stats.to_dict(orient="index")
    
    global_stats = {
        "global_mean": df["TOTAL_EARNINGS"].mean().round(2),
        "global_median": df["TOTAL_EARNINGS"].median().round(2),
        "global_min": df["TOTAL_EARNINGS"].min().round(2),
        "global_max": df["TOTAL_EARNINGS"].max().round(2),
        "global_std": df["TOTAL_EARNINGS"].std().round(2),
        "total_employees": len(df)
    }
    
    result["global"] = global_stats
    return result
