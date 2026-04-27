import requests
import pandas as pd
from pathlib import Path

INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"

def get_status():
    r = requests.get(STATUS_URL)
    stations = r.json()["data"]["stations"]
    df = pd.json_normalize(stations)
    df["mechanical"] = df["num_bikes_available_types"].str[0].str.get("mechanical").fillna(0)
    df["ebike"] = df["num_bikes_available_types"].str[1].str.get("ebike").fillna(0)
    return df[["station_id", "num_bikes_available", "num_docks_available", "mechanical", "ebike"]]

def main():
    df = get_status()
    df["timestamp"] = pd.Timestamp.now().floor("10min")
    df = df.rename(columns={
        "num_bikes_available": "total",
        "num_docks_available": "docks"
    })

    # Un fichier Parquet par collecte, nommé par timestamp
    Path("data").mkdir(exist_ok=True)
    filename = f"data/velib_{df['timestamp'].iloc[0].strftime('%Y%m%d_%H%M')}.parquet"
    df.to_parquet(filename, engine="pyarrow", index=False)
    print(f"Sauvegardé : {filename}")

if __name__ == "__main__":
    main()
