-- Catalogs
CREATE CATALOG IF NOT EXISTS dev_01_bronze;
CREATE CATALOG IF NOT EXISTS dev_02_silver;
CREATE CATALOG IF NOT EXISTS dev_03_gold;

-- Schemas
CREATE SCHEMA IF NOT EXISTS dev_01_bronze.statsbomb;
CREATE SCHEMA IF NOT EXISTS dev_02_silver.sport_technical_statsbomb;
CREATE SCHEMA IF NOT EXISTS dev_03_gold.game_analysis;