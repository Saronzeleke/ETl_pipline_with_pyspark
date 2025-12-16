import requests
import json
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import config


class DataExtractor:
    """
    Robust data extraction class for the ETL pipeline.
    Handles:
      1. Parquet data (NYC Taxi)
      2. CSV data (Customer metadata)
      3. JSON data (Weather)
    Ensures folders exist, caches locally, and loads Spark DataFrames.
    """

    def __init__(self):
        # Ensure all directories exist
        config._create_directories()

        self.spark = self._create_spark_session()

        self.sources = {
            "parquet": {
                "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
                "local": config.RAW_DATA_DIR / "nyc_taxi_2023-01.parquet"
            },
            "csv": {
                "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv",
                "local": config.RAW_DATA_DIR / "customers.csv"
            },
            "weather": {
                "url": "https://archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01&start_date=2020-01-01&end_date=2023-12-31&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=America/New_York",
                "local": config.RAW_DATA_DIR / "nyc_weather.json"
            }
        }

    # ----------------------
    # Spark Session
    # ----------------------
    def _create_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(config.SPARK_CONFIG.get("app.name", "ETL_Pipeline"))
            .master("local[*]")
            .config("spark.driver.memory", config.SPARK_CONFIG.get("spark.driver.memory", "2g"))
            .config("spark.executor.memory", config.SPARK_CONFIG.get("spark.executor.memory", "2g"))
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", config.SPARK_CONFIG.get("spark.sql.shuffle.partitions", 2))
            .config("spark.hadoop.io.native.lib", "false")  # Disable native Hadoop to avoid Windows errors
            .getOrCreate()
        )
        logger.info(f"✅ Spark session initialized (v{spark.version})")
        return spark

    # ----------------------
    # Download helpers
    # ----------------------
    def _download_file(self, url: str, local_path: Path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if local_path.is_file():
            logger.info(f"File exists, skipping download: {local_path}")
            return

        logger.info(f"Downloading {url} → {local_path}")
        try:
            with requests.get(url.strip(), stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            logger.success(f"✅ Downloaded: {local_path}")
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    def _download_json(self, url: str, local_path: Path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if local_path.is_file():
            logger.info(f"JSON already exists: {local_path}")
            return

        logger.info(f"Fetching JSON from API: {url}")
        try:
            resp = requests.get(url.strip(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.success(f"✅ Saved JSON: {local_path}")
        except Exception as e:
            logger.error(f"Failed to fetch JSON from {url}: {e}")
            raise

    # ----------------------
    # Extract all sources
    # ----------------------
    def extract_all_sources(self) -> dict:
        """
        Downloads (if needed) and loads all sources as Spark DataFrames:
        - trips_df (Parquet)
        - customers_df (CSV)
        - weather_df (JSON, flattened)
        Returns a dict: {"trips", "customers", "weather"}
        """
        logger.info("🔍 Extracting all sources...")

        # 1️⃣ Parquet - NYC Taxi Trips
        p = self.sources["parquet"]
        self._download_file(p["url"], p["local"])
        trips_df = self.spark.read.parquet(str(p["local"]))
        logger.info(f"✅ Loaded Parquet: {len(trips_df.columns)} columns, approx. {trips_df.count()} rows")

        # 2️⃣ CSV - Customer Metadata
        c = self.sources["csv"]
        self._download_file(c["url"], c["local"])
        customers_df = self.spark.read.option("header", True).option("inferSchema", True).csv(str(c["local"]))
        logger.info(f"✅ Loaded CSV: {len(customers_df.columns)} columns, approx. {customers_df.count()} rows")

        # 3️⃣ JSON - Weather Data
        w = self.sources["weather"]
        self._download_json(w["url"], w["local"])
        weather_raw_df = self.spark.read.option("multiLine", True).json(str(w["local"]))

        # Flatten the daily arrays into rows
        weather_df = weather_raw_df.select(
            F.explode("daily.time").alias("date"),
            F.col("daily.temperature_2m_max").alias("temp_max"),
            F.col("daily.temperature_2m_min").alias("temp_min"),
            F.col("daily.precipitation_sum").alias("precip")
        )
        logger.info(f"✅ Loaded & flattened JSON: {len(weather_df.columns)} columns, approx. {weather_df.count()} rows")

        logger.success("✅ All sources extracted successfully!")
        return {"trips": trips_df, "customers": customers_df, "weather": weather_df}
