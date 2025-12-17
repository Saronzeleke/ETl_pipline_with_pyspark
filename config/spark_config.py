import os
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession
from config.settings import config

def create_spark_session(app_name=None):
    # JAVA_HOME already set in config
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