# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "37c30f11-c484-4314-b285-7f78241ee57c",
# META       "default_lakehouse_name": "Lakehouse_1",
# META       "default_lakehouse_workspace_id": "9e85b650-2da3-4cba-888d-3b05610ead09",
# META       "known_lakehouses": [
# META         {
# META           "id": "37c30f11-c484-4314-b285-7f78241ee57c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import json
import os
import re
from datetime import datetime, timezone


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


place = "Oslo"

headers = {
    "User-Agent": "TestApplication/1.0 jacob.lindberg@intility.no"
}

params = {
    "lat": 59.91,
    "lon": 10.75
}

r = requests.get(
    "https://api.met.no/weatherapi/locationforecast/2.0/complete",
    headers=headers,
    params=params,
)

r.raise_for_status()
weather_json = r.json()

now = datetime.now(timezone.utc)
safe_place = re.sub(r"[^a-zA-Z0-9_-]", "_", place.lower())

folder_path = (
    f"/lakehouse/default/Files/bronze/MET/weather/"
    f"place={safe_place}/"
    f"year={now:%Y}/"
    f"month={now:%m}/"
    f"day={now:%d}"
)

file_name = f"{safe_place}_weather_{now:%Y%m%d_%H%M%S}.json"
full_path = f"{folder_path}/{file_name}"

os.makedirs(folder_path, exist_ok=True)

with open(full_path, "w", encoding="utf-8") as f:
    json.dump(weather_json, f, ensure_ascii=False, indent=2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
