import os
from pathlib import Path
from loguru import logger

class Config:
    # ----------------------
    # PROJECT PATHS
    # ----------------------
    BASE_DIR = Path(__file__).parent.parent.parent.resolve()
    DATA_DIR = BASE_DIR / "ETL_PIPLINE_WITH_PYSPARK/data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    OUTPUT_DIR = DATA_DIR / "output"
    DUCKDB_PATH = OUTPUT_DIR / "analytics.db"
    # ----------------------
    # SPARK CONFIG
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
    # WINDOWS SUPPORT (consolidated here)
    # ----------------------
    if os.name == "nt":
        SPARK_HOME = r"C:\spark\spark-3.5.7-bin-hadoop3"
        JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
        HADOOP_HOME = r"C:\hadoop"
        os.environ["SPARK_HOME"] = SPARK_HOME
        os.environ["JAVA_HOME"] = JAVA_HOME
        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["PATH"] = f"{HADOOP_HOME}\\bin;{SPARK_HOME}\\bin;{os.environ.get('PATH', '')}"
        os.environ["HADOOP_USER_NAME"] = "admin"  # Bypass permissions
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

# Global config instance
config = Config()