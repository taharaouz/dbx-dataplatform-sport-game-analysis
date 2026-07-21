from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = "dev_01_bronze"
SILVER = "dev_02_silver"

BRONZE_SCHEMA = "statsbomb"
SILVER_SCHEMA = "sport_technical_statsbomb"


dp.create_streaming_table(
    name=f"{SILVER}.{SILVER_SCHEMA}.competitions",
    comment=(
        "StatsBomb competitions, one row per (competition_id, season_id). "
        "SCD1 — latest version per key."
    ),
    table_properties={
        "quality": "silver",
    },
    expect_all_or_drop={
        "valid_competition_id": "competition_id IS NOT NULL",
        "valid_season_id": "season_id IS NOT NULL",
    },
)


# --- Staging view: pass-through (competitions is already flat) -------------------
@dp.temporary_view(name="competitions_stg")
def competitions_stg():
    return spark.readStream.table(f"{BRONZE}.{BRONZE_SCHEMA}.competitions")


# --- CDC flow --------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target=f"{SILVER}.{SILVER_SCHEMA}.competitions",
    source="competitions_stg",
    keys=["competition_id", "season_id"],
    sequence_by="_ingest_timestamp",
    stored_as_scd_type=1,
)