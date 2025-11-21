import json

import pandas as pd
import requests

CITIES_MAP = {
    9: "Bekasi",
    8: "Tangerang",
    7: "Bogor",
    5: "Jakarta Utara",
    3: "Jakarta Selatan",
    4: "Jakarta Timur",
    2: "Jakarta Pusat",
    1: "Jakarta Barat"
}


def fetch_jakarta_commodity_by_cities(city_id: int, year_month: str = "2025-11") -> pd.DataFrame | None:
    api = f"https://infopangan.jakarta.go.id/api2/v1/public/report?filterBy=city&Id={city_id}&yearMonth={year_month}"

    city_name = CITIES_MAP.get(city_id, f"ID {city_id} (Unknown)")
    print(f"Fetch data from {api}")
    try:
        response = requests.get(api)
        response.raise_for_status()

        try:
            json_data = response.json()
        except json.JSONDecodeError:
            print("Failed to decode JSON Response")
            return None
        if "data" not in json_data:
            print("Error: Top-Level key 'data' ")
            return None
        data_response = json_data["data"]["data"]
        if not data_response:
            print(f"Warning: No Commodity data")
        data = pd.json_normalize(
            data=data_response,
            record_path=["recaps"],
            meta=[
                'commodity_id',
                'commodity_name',
                'avg_value',
                'max_value',
                'min_value'
            ],
            errors='ignore'
        )
        data.rename(
            columns={
                "value":  "daily_price",
                "time": "date",
            }
        )
        data["city_id"] = city_id
        data["city_name"] = city_name
        return data
    except Exception as e:
        print(f"{e}")
    return None

def fetch_all_cities_data(cities: dict, year_month: str = "2025-11") -> pd.DataFrame:
    all_data = []
    for city_id, city_name in cities.items():
        print(f"[{city_id}] Fetch data for {city_name}")
        df = fetch_jakarta_commodity_by_cities(city_id, year_month)
        if df is not None and not df.empty:
            all_data.append(df)
            print(f"Successfully fetched data for {city_name} {len(df)} cities")
        elif df is None:
            print(f"Could not retrieve data for {city_name}")
        else:
            print(f"Data was empty for {city_name}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        print(f"SUCCESS: Total daily records combined: {len(final_df)}")
        return final_df
    else:
        print("WARNING: No data was successfully retrieved from any city.")
        return pd.DataFrame()
    pass

def fetch_all_month_data(year: int, cities: dict)-> pd.DataFrame:
    all_yearly_data = []

    for month in range(1, 13):
        month_str = str(month).zfill(2)
        target_month = f"{year}-{month_str}"
        print(f"Fetching data for {target_month}")
        print(f"\n=======================================================")
        print(f"MULAI AMBIL DATA BULAN: {target_month}")
        print(f"=======================================================")

        month_df = fetch_all_cities_data(cities, target_month)
        if not month_df.empty:
            month_df["Year"] = year
            month_df["Month"] = month
            all_yearly_data.append(month_df)
        else:
            print(f"Could not retrieve data for {target_month}")

    if all_yearly_data:
        final_df = pd.concat(all_yearly_data, ignore_index=True)
        return final_df


if __name__ == "__main__":
    target_month = "2025-11"
    target_year = 2024
    combined_df = fetch_all_month_data(target_year, CITIES_MAP)
    combined_df.to_csv("cities_2025.csv", index=False)
