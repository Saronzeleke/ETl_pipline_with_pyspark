import sys
from pathlib import Path
import os

# Ensure src is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

# ----------------------
# SAFE IMPORTS
# ----------------------
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DuckDBLoader
from config.settings import config

# ======================
# TASKS
# ======================
@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=10)
def extract_data():
    """Extract raw datasets"""
    logger.info("Initializing DataExtractor...")
    extractor = DataExtractor()
    raw_data = extractor.extract_all_sources()
    logger.info(
        f"Extracted rows → "
        f"trips={raw_data['trips'].count()}, "
        f"zones={raw_data['zones'].count()}, "
        f"weather={raw_data['weather'].count()}"
    )
    # Save raw datasets explicitly
    raw_trips_path = config.RAW_DATA_DIR / "trips.parquet"
    raw_zones_path = config.RAW_DATA_DIR / "taxi_zone_lookup.parquet"
    raw_weather_path = config.RAW_DATA_DIR / "weather.parquet"
    raw_data['trips'].write.mode("overwrite").parquet(str(raw_trips_path))
    raw_data['zones'].write.mode("overwrite").parquet(str(raw_zones_path))
    raw_data['weather'].write.mode("overwrite").parquet(str(raw_weather_path))
    logger.info(f"✅ Raw datasets saved to {config.RAW_DATA_DIR}")
    return raw_data, extractor.spark

@task(cache_policy=NO_CACHE)
def transform_data(trips_df, zones_df, weather_df, spark):
    """Clean, enrich, integrate zones/weather, and aggregate trips"""
    logger.info("Initializing DataTransformer...")
    transformer = DataTransformer(spark)
    # 1️⃣ Clean & enrich trips
    cleaned = transformer.clean_data(trips_df)
    enriched = transformer.add_derived_columns(cleaned)
    # 2️⃣ Integrate zones & weather
    integrated = transformer.integrate_datasets(enriched, zones_df, weather_df)
    # 3️⃣ Save processed dataset
    processed_path = config.PROCESSED_DATA_DIR / "trips_processed.parquet"
    integrated.write.mode("overwrite").parquet(str(processed_path))
    logger.info(f"✅ Processed dataset saved → {processed_path}")
    # 4️⃣ Aggregate
    aggregated = transformer.aggregate_data(integrated)
    for table_name, df in aggregated.items():
        table_path = config.PROCESSED_DATA_DIR / f"{table_name}.parquet"
        df.write.mode("overwrite").parquet(str(table_path))
        logger.info(f"✅ Aggregated dataset saved → {table_path}")
    return aggregated

@task(cache_policy=NO_CACHE)
def load_data(aggregated):
    """Load analytics tables into DuckDB and create views"""
    logger.info("Initializing DuckDBLoader...")
    loader = DuckDBLoader()
    results = {}
    try:
        # Load aggregated tables into DuckDB
        for table_name, df in aggregated.items():
            row_count = loader.write_spark_to_duckdb(df, table_name)
            results[table_name] = row_count
        # Export all aggregated tables to Parquet for BI
        for table_name in aggregated:
            loader.export_to_parquet(table_name)
        # Create analytical views in DuckDB
        loader.create_views()
        tables = loader.list_tables()
        return {
            "tables": tables,
            "results": results,
            "db_path": str(loader.db_path)
        }
    finally:
        loader.close()

# ======================
# FLOW
# ======================
@flow(
    name="ETL Pipeline - PySpark + DuckDB",
    description="Production-grade ETL with Spark, DuckDB, Prefect, and BI output",
    version="1.0",
    log_prints=True
)
def run_pipeline():
    logger.info("🚀 Starting ETL Pipeline")
    spark = None
    try:
        logger.info("📥 EXTRACTION")
        raw_data, spark = extract_data()
        logger.info("🔄 TRANSFORMATION")
        aggregated = transform_data(
            raw_data["trips"],
            raw_data["zones"],
            raw_data["weather"],
            spark
        )
        logger.info("💾 LOADING")
        load_result = load_data(aggregated)
        logger.info("📊 FINAL REPORT")
        logger.info(f"Database: {load_result['db_path']}")
        logger.info(f"Tables created: {len(load_result['tables'])}")
        for table, count in load_result["results"].items():
            logger.info(f" - {table}: {count} rows")
        return {
            "status": "success",
            "tables": load_result["tables"],
            "database": load_result["db_path"]
        }
    except Exception as e:
        logger.error(f"💥 PIPELINE FAILURE: {e}")
        return {"status": "failed", "error": str(e)}
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark stopped")

# ======================
# ENTRY POINT
# ======================
if __name__ == "__main__":
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <level>{message}</level>",
        level="INFO"
    )
    result = run_pipeline()
    sys.exit(0 if result.get("status") == "success" else 1)