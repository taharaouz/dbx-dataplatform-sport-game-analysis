from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, LongType,
)

BRONZE = "dev_01_bronze"
SILVER = "dev_02_silver"

BRONZE_SCHEMA = "statsbomb"
SILVER_SCHEMA = "sport_technical_statsbomb"


# Minimal schemas — only what we need to reach into for parsing.
POSITION_STRUCT = StructType([
    StructField("position_id", IntegerType()),
    StructField("position", StringType()),
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("from_period", IntegerType()),
    StructField("to_period", IntegerType()),
    StructField("start_reason", StringType()),
    StructField("end_reason", StringType()),
    StructField("counterpart_id", LongType()),
    StructField("counterpart_name", StringType()),
])
POSITIONS_SCHEMA = ArrayType(POSITION_STRUCT)

COUNTRY_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])


dp.create_streaming_table(
    name=f"{SILVER}.{SILVER_SCHEMA}.lineups",
    comment=(
        "StatsBomb match lineups, one row per (match_id, player_id). "
        "SCD1 — latest version per key. Also carries the team-level events/"
        "formations arrays consumed by the derived silver tables."
    ),
    table_properties={
        "quality": "silver",
    },
    expect_all_or_drop={
        "valid_match_id": "match_id IS NOT NULL",
        "valid_player_id": "player_id IS NOT NULL",
    },
)


# --- Staging view: parse country & positions, pass everything else through -------
@dp.temporary_view(name="lineups_stg")
def lineups_stg():
    df = spark.readStream.table(f"{BRONZE}.{BRONZE_SCHEMA}.lineups")

    return (
        df
        .withColumn("positions", F.from_json("positions", POSITIONS_SCHEMA))
        .withColumn("country", F.from_json("country", COUNTRY_SCHEMA))
    )


# --- CDC flow --------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target=f"{SILVER}.{SILVER_SCHEMA}.lineups",
    source="lineups_stg",
    keys=["match_id", "player_id"],
    sequence_by="_ingest_timestamp",
    stored_as_scd_type=1,
)