# ingest_statsbomb_bronze.py
# Ingest free StatsBomb open data (competitions, matches, lineups) into the bronze layer.

from datetime import datetime, timezone
from statsbombpy import sb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()

BRONZE_CATALOG = "dev_01_bronze"
BRONZE_SCHEMA = "statsbomb"
INGEST_TS = datetime.now(timezone.utc)

EURO_2024_COMPETITION_ID = 55
EURO_2024_SEASON_ID = 282


def write_bronze(pdf, table_name):
    """Convert a pandas DataFrame to Spark, add ingestion metadata, and overwrite the bronze table."""
    if pdf is None or pdf.empty:
        print(f"  [skip] {table_name}: no data")
        return

    # Cast all columns to string to avoid schema-inference conflicts across matches (bronze = raw).
    pdf = pdf.astype(str)

    sdf = (
        spark.createDataFrame(pdf)
        .withColumn("_ingest_timestamp", F.lit(INGEST_TS.isoformat()).cast("timestamp"))
        .withColumn("_source", F.lit("statsbombpy_open_data"))
    )

    target = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    sdf.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)
    print(f"  [ok] {target}: {sdf.count()} rows")


# 1. COMPETITIONS ----------------------------------------------------------
print("Ingesting competitions...")
competitions = sb.competitions()
# Keep only the Euro 2024 row for the bronze competitions table.
competitions = competitions[
    (competitions["competition_id"] == EURO_2024_COMPETITION_ID)
    & (competitions["season_id"] == EURO_2024_SEASON_ID)
]
write_bronze(competitions, "competitions")

# 2. MATCHES ---------------------------------------------------------------
print("Ingesting matches...")
matches = sb.matches(
    competition_id=EURO_2024_COMPETITION_ID,
    season_id=EURO_2024_SEASON_ID,
)
write_bronze(matches, "matches")

# 3. LINEUPS ---------------------------------------------------------------
# One lineups call per match_id.
print("Ingesting lineups...")
lineups_frames = []
for match_id in matches["match_id"].unique():
    try:
        l = sb.lineups(match_id=match_id)  # returns {team_name: DataFrame}
        for team_name, team_df in l.items():
            team_df = team_df.copy()
            team_df["match_id"] = match_id
            team_df["team_name"] = team_name
            lineups_frames.append(team_df)
    except Exception as e:
        print(f"  [warn] lineups {match_id}: {e}")

lineups = pd.concat(lineups_frames, ignore_index=True) if lineups_frames else pd.DataFrame()
write_bronze(lineups, "lineups")

print("Done.")