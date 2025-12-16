import os
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession
from config.settings import config


def find_java():
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home).exists():
        java_bin = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if java_bin.exists():
            logger.info(f"Java found: {java_bin}")
            return str(java_bin)
    raise EnvironmentError("JAVA_HOME not set correctly")


def create_spark_session(app_name=None):
    find_java()

    spark_conf = config.SPARK_CONFIG.copy()
    if app_name:
        spark_conf["app.name"] = app_name

    builder = SparkSession.builder.appName(spark_conf["app.name"]).master("local[*]")

    for k, v in spark_conf.items():
        if k.startswith("spark."):
            builder = builder.config(k, v)

    if os.name == "nt":
        builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")

    spark = builder.getOrCreate()
    logger.info(f"Spark started (v{spark.version})")

    return spark
