from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

BRONZE = "dev_01_bronze"
SILVER = "dev_02_silver"

BRONZE_SCHEMA = "statsbomb"
SILVER_SCHEMA = "sport_technical_statsbomb"


# Minimal schemas — only the nested objects worth unpacking.
COMPETITION_STAGE_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])


dp.create_streaming_table(
    name=f"{SILVER}.{SILVER_SCHEMA}.matches",
    comment=(
        "StatsBomb matches, one row per match_id. "
        "SCD1 — latest version per key."
    ),
    table_properties={
        "quality": "silver",
    },
    expect_all_or_drop={
        "valid_match_id": "match_id IS NOT NULL",
    },
)


# --- Staging view: parse competition_stage, pass everything else through ---------
@dp.temporary_view(name="matches_stg")
def matches_stg():
    df = spark.readStream.table(f"{BRONZE}.{BRONZE_SCHEMA}.matches")

    return (
        df
        .withColumn(
            "competition_stage",
            F.from_json("competition_stage", COMPETITION_STAGE_SCHEMA),
        )
    )


# --- CDC flow --------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target=f"{SILVER}.{SILVER_SCHEMA}.matches",
    source="matches_stg",
    keys=["match_id"],
    sequence_by="_ingest_timestamp",
    stored_as_scd_type=1,
)