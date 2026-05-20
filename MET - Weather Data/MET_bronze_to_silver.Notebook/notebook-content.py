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

from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

place = "oslo"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

now_utc = datetime.now(timezone.utc)

process_time = now_utc.replace(minute=0, second=0, microsecond=0)

year = process_time.strftime("%Y")
month = process_time.strftime("%m")
day = process_time.strftime("%d")
hour = process_time.strftime("%H")

json_path = (
    f"Files/bronze/MET/weather/"
    f"place={place}/year={year}/month={month}/day={day}/hour={hour}/"
)

print(json_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("multiline", "true").json(json_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

historical_temp_df = (
    raw_df
    .select(
        F.explode("properties.timeseries").alias("ts")
    )
    .select(
        F.lit(process_time).cast("timestamp").alias("time"),
        F.col("ts.data.instant.details.air_temperature").alias("temperature")
    )
    .orderBy("time")
    .limit(1)
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    historical_temp_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("weather_temperature_history")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
