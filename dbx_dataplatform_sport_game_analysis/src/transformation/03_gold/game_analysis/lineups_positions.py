from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = "dev_02_silver"
GOLD = "dev_03_gold"

SILVER_SCHEMA = "sport_technical_statsbomb"
GOLD_SCHEMA = "game_analysis"


@dp.materialized_view(
    name=f"{GOLD}.{GOLD_SCHEMA}.lineups_positions",
    comment=(
        "Exploded StatsBomb lineup positions — one row per "
        "(match_id, player_id, position stint). Derived from the silver "
        "lineups.positions array."
    ),
    table_properties={
        "quality": "gold",
    },
)
def lineups_positions():
    df = spark.read.table(f"{SILVER}.{SILVER_SCHEMA}.lineups")

    # One row per element of the positions array; drop players who never
    # took a position on the pitch (unused subs -> empty/null array).
    exploded = df.select(
        F.col("match_id"),
        #F.col("team_id"),
        F.col("team_name"),
        F.col("player_id"),
        F.col("player_name"),
        F.explode("positions").alias("position"),
    )

    return exploded.select(
        "match_id",
        #"team_id",
        "team_name",
        "player_id",
        "player_name",
        F.col("position.position_id").alias("position_id"),
        F.col("position.position").alias("position"),
        F.col("position.from").alias("from_time"),
        F.col("position.to").alias("to_time"),
        F.col("position.from_period").alias("from_period"),
        F.col("position.to_period").alias("to_period"),
        F.col("position.start_reason").alias("start_reason"),
        F.col("position.end_reason").alias("end_reason"),
        F.col("position.counterpart_id").alias("counterpart_id"),
        F.col("position.counterpart_name").alias("counterpart_name"),
    )