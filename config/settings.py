import os
from pathlib import Path
from loguru import logger


class Config:
    # ----------------------
    # PROJECT PATHS
    # ----------------------
    BASE_DIR = Path(__file__).parent.parent.parent.resolve()
    DATA_DIR = BASE_DIR / "ETl_pipline_with_pyspark/data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    OUTPUT_DIR = DATA_DIR / "output"

    # ----------------------
    # DUCKDB
    # ----------------------
    DUCKDB_PATH = OUTPUT_DIR / "analytics.db"

    # ----------------------
    # SPARK CONFIG (USED BY extractor)
    # ----------------------
    SPARK_CONFIG = {
        "app.name": "ETL_Pipeline",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.sql.shuffle.partitions": "2",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }

    # ----------------------
    # WINDOWS SUPPORT
    # ----------------------
    if os.name == "nt":
        SPARK_DIST_HOME = r"C:\spark\spark-3.5.7-bin-hadoop3"
        JAVA_HOME_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"

        os.environ.setdefault("SPARK_HOME", SPARK_DIST_HOME)
        os.environ.setdefault("JAVA_HOME", JAVA_HOME_PATH)
        os.environ["PATH"] = SPARK_DIST_HOME + r"\bin;" + os.environ.get("PATH", "")

        SPARK_CONFIG["spark.sql.warehouse.dir"] = str(
            OUTPUT_DIR / "spark-warehouse"
        )

    # ----------------------
    # ETL SETTINGS
    # ----------------------
    ETL_CONFIG = {
        "enable_prefect": True,
        "log_level": "INFO",
    }

    # ----------------------
    # INIT
    # ----------------------
    def __init__(self):
        self._create_directories()
        self._log_config()

    def _create_directories(self):
        for d in [
            self.DATA_DIR,
            self.RAW_DATA_DIR,
            self.PROCESSED_DATA_DIR,
            self.OUTPUT_DIR,
        ]:
            d.mkdir(parents=True, exist_ok=True)

    def _log_config(self):
        logger.info(f"BASE_DIR: {self.BASE_DIR}")
        logger.info(f"RAW_DATA_DIR: {self.RAW_DATA_DIR}")
        logger.info(f"DUCKDB_PATH: {self.DUCKDB_PATH}")


# Global config instance (USED EVERYWHERE)
config = Config()
