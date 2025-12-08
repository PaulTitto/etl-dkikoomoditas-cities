import json
import time

import pandas as pd
import requests

CITIES_MAP = {
    # 9: "Bekasi",
    # 8: "Tangerang",
    # 7: "Bogor",
    5: "Jakarta Utara",
    3: "Jakarta Selatan",
    4: "Jakarta Timur",
    2: "Jakarta Pusat",
    1: "Jakarta Barat"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Referer": "https://infopangan.jakarta.go.id/harga-pangan",
    "Origin": "https://infopangan.jakarta.go.id",
    "Connection": "keep-alive"
}


def fetch_jakarta_commodity_by_cities(city_id: int, year_month: str = "2025-11") -> pd.DataFrame | None:
    api = f"https://infopangan.jakarta.go.id/api2/v1/public/report?filterBy=city&Id={city_id}&yearMonth={year_month}"

    city_name = CITIES_MAP.get(city_id, f"ID {city_id} (Unknown)")
    print(f"Fetch data from {api}")
    try:
        response = requests.get(api, headers=HEADERS, verify=False, timeout=5)
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
        data = data.rename(columns={"value": "daily_price", "time": "date"})
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
        time.sleep(1)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        print(f"SUCCESS: Total daily records combined: {len(final_df)}")
        return final_df
    else:
        print("WARNING: No data was successfully retrieved from any city.")
        return pd.DataFrame()
    pass


def fetch_all_month_data(year: int, cities: dict) -> pd.DataFrame:
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
    # target_month = "2025-11"
    # target_year_2025 = 2025
    target_year_2023 = 2023
    # combined_df_2024 = fetch_all_month_data(target_year_2024, CITIES_MAP)
    # combined_df_2024.to_csv("cities_2024.csv", index=False)
    combined_df_2023 = fetch_all_month_data(target_year_2023, CITIES_MAP)
    combined_df_2023.to_csv("cities_2023.csv", index=False)

    # month1 = fetch_all_cities_data(CITIES_MAP, "2025-01")
    # month2 = fetch_all_cities_data(CITIES_MAP, "2025-02")
    # month3 = fetch_all_cities_data(CITIES_MAP, "2025-03")
    # month4 = fetch_all_cities_data(CITIES_MAP, "2025-04")
    # month5 = fetch_all_cities_data(CITIES_MAP, "2025-05")
    # month6 = fetch_all_cities_data(CITIES_MAP, "2025-06")
    # month7 = fetch_all_cities_data(CITIES_MAP, "2025-07")
    # month8 = fetch_all_cities_data(CITIES_MAP, "2025-08")
    # month9 = fetch_all_cities_data(CITIES_MAP, "2025-09")
    # month10 = fetch_all_cities_data(CITIES_MAP, "2025-10")
    # month11 = fetch_all_cities_data(CITIES_MAP, "2025-11")
    # month12 = fetch_all_cities_data(CITIES_MAP, "2025-12")

    # month1.to_csv("cities_2025-01.csv", index=False)

    # dfs_to_concat = []
    # if not combined_df_2024.empty:
    #     dfs_to_concat.append(combined_df_2024)
    # if not combined_df_2025.empty:
    #     dfs_to_concat.append(combined_df_2025)
    #
    # if dfs_to_concat:
    #     combined_df = pd.concat(dfs_to_concat, ignore_index=True)
    #     filename = "jakarta_commodity_2024_2025.csv"
    #     combined_df.to_csv(filename, index=False)
    #     print(f"\nSUCCESS: Saved {len(combined_df)} records to {filename}")
    # else:
    #     print("\nFAILED: No data collected.")
