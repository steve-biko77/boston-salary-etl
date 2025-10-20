
import pandas as pd
import pytest
from etl import extract_boston_salary, transform_boston_salary, load, analyse

URL = "https://data.boston.gov/api/3/action/datastore_search?resource_id=31358fd1-849a-48e0-8285-e813f6efbdf1"

def test_extract_returns_dataframe():
    """Teste que extract_boston_salary retourne un DataFrame non vide."""
    df = extract_boston_salary(URL)
    assert isinstance(df, pd.DataFrame), "L'extraction ne retourne pas un DataFrame"
    assert not df.empty, "Le DataFrame extrait est vide"
    assert "DEPARTMENT_NAME" in df.columns, "La colonne DEPARTMENT_NAME est absente"
    assert any(col in df.columns for col in ["TOTAL EARNINGS", "GAINS TOTAUX", "TOTAL GAINS", "TOTAL DES GAIN", "TOTAL DES GAINS"]), "Aucune colonne de gains totaux n'est présente"

def test_transform_converts_total_earnings():
    """Teste que transform_boston_salary convertit TOTAL_EARNINGS en float."""
    data = pd.DataFrame({
        "NAME": ["Test"],
        "DEPARTMENT_NAME": ["Service de police de Boston"],
        "GAINS TOTAUX": ["95 184,68"],
        "RÉGULIER": ["95 184,68"],
        "RÉTRO": ["0"],
        "AUTRE": ["0"],
        "HEURES SUPPLÉMENTAIRES": ["0"],
        "BLESSÉ": ["0"],
        "DÉTAIL": ["0"],
        "QUINN/ÉDUCATION INCITATION": ["0"]
    })
    df_transformed = transform_boston_salary(data)
    assert "TOTAL_EARNINGS" in df_transformed.columns, "La colonne TOTAL_EARNINGS est absente"
    assert df_transformed["TOTAL_EARNINGS"].dtype == float, "TOTAL_EARNINGS n'est pas de type float"
    assert df_transformed["TOTAL_EARNINGS"].iloc[0] == 95184.68, "Conversion de TOTAL_EARNINGS incorrecte"

def test_analyse_returns_dict():
    """Teste que analyse retourne un dictionnaire avec des statistiques."""
    data = pd.DataFrame({
        "DEPARTMENT_NAME": ["Service de police de Boston", "Service de police de Boston", "Fire"],
        "TOTAL_EARNINGS": [100000.0, 150000.0, 120000.0]
    })
    result = analyse(data)
    assert isinstance(result, dict), "L'analyse ne retourne pas un dictionnaire"
    assert "Service de police de Boston" in result, "Les statistiques par département sont manquantes"
    assert "global" in result, "Les statistiques globales sont manquantes"
    assert result["Service de police de Boston"]["mean"] == 125000.0, "Moyenne par département incorrecte"
    assert result["global"]["global_mean"] == 123333.33, "Moyenne globale incorrecte"
