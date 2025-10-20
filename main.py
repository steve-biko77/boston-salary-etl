
from etl import extract_boston_salary, transform_boston_salary, load, analyse

if __name__ == "__main__":
    url = "https://data.boston.gov/api/3/action/datastore_search?resource_id=31358fd1-849a-48e0-8285-e813f6efbdf1"
    df = extract_boston_salary(url)
    df_clean = transform_boston_salary(df)
    load(df_clean)
    stats = analyse(df_clean)
    print("Statistiques par département :")
    for dept, values in stats.items():
        print(f"{dept}: {values}")
