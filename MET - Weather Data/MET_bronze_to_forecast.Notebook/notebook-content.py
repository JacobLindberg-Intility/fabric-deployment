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
from delta.tables import DeltaTable

place = "oslo"

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

raw_df = spark.read.option("multiline", "true").json(json_path)

forecast_24h_df = (
    raw_df
    .withColumn("ts", F.explode("properties.timeseries"))
    .select(
        F.lit(place).alias("place"),
        F.to_timestamp("properties.meta.updated_at").alias("met_updated_at"),
        F.to_timestamp("ts.time").alias("forecast_time_utc"),
        F.col("ts.data.instant.details.air_temperature").alias("air_temperature"),
        F.current_timestamp().alias("ingested_at_utc"),
        F.lit(json_path).alias("source_path")
    )
    .where(
        (F.col("forecast_time_utc") >= F.lit(process_time.strftime("%Y-%m-%dT%H:%M:%SZ")).cast("timestamp")) &
        (F.col("forecast_time_utc") < F.lit((process_time + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")).cast("timestamp"))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



table_name = "weather_forecast_next_24h"

if spark.catalog.tableExists(table_name):
    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("t")
        .merge(
            forecast_24h_df.alias("s"),
            """
            t.place = s.place
            AND t.forecast_time_utc = s.forecast_time_utc
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        forecast_24h_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

spark.sql(f"""
DELETE FROM {table_name}
WHERE forecast_time_utc < timestamp('{process_time.strftime("%Y-%m-%d %H:%M:%S")}')
   OR forecast_time_utc >= timestamp('{(process_time + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")}')
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
