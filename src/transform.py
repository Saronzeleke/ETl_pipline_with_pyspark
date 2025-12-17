from pyspark.sql import types as T
from pyspark.sql import functions as F
from loguru import logger

class DataTransformer:
    def __init__(self, spark):
        self.spark = spark

    # ----------------------
    # CLEANING (TRIPS ONLY)
    # ----------------------
    def clean_data(self, df):
        logger.info("Starting data cleaning...")
        df = df.dropDuplicates()
        df = df.fillna({
            "VendorID": 0,
            "passenger_count": 0,
            "trip_distance": 0.0,
            "fare_amount": 0.0,
            "total_amount": 0.0
        })
        df = df.filter(
            (F.col("trip_distance") > 0) &
            (F.col("fare_amount") > 0)
        )
        return df

    # ----------------------
    # DERIVED COLUMNS
    # ----------------------
    def add_derived_columns(self, df):
        logger.info("Adding derived columns...")
        df = df.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60
        )
        df = (
            df
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
            .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        )
        df = df.withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60)
            ).otherwise(0)
        )
        df = df.withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount") / F.col("trip_distance")
            ).otherwise(0)
        )
        return df

    # ----------------------
    # WEATHER PREPARATION
    # ----------------------
    def prepare_weather(self, weather_df):
        """
        Prepare weather DataFrame for joining.
        Handles both flattened and raw JSON formats.
        """
        logger.info("Preparing weather data...")
        if 'date' in weather_df.columns:
            # Already flattened, cast types
            logger.info("Weather already flattened, casting types...")
            return weather_df \
                .withColumn("date", F.to_date(F.col("date"))) \
                .withColumn("temp_max", F.col("temp_max").cast("double")) \
                .withColumn("temp_min", F.col("temp_min").cast("double")) \
                .withColumn("precip", F.col("precip").cast("double")) \
                .select("date", "temp_max", "temp_min", "precip")
        else:
            # Raw JSON format: flatten it
            logger.info("Flattening raw weather JSON...")
            weather_flat = weather_df.select(
                F.explode(
                    F.arrays_zip(
                        F.col("daily.time"),
                        F.col("daily.temperature_2m_max"),
                        F.col("daily.temperature_2m_min"),
                        F.col("daily.precipitation_sum")
                    )
                ).alias("daily")
            ).select(
                F.to_date(F.col("daily.time")).alias("date"),
                F.col("daily.temperature_2m_max").alias("temp_max"),
                F.col("daily.temperature_2m_min").alias("temp_min"),
                F.col("daily.precipitation_sum").alias("precip")
            )
            logger.info(f"Weather exploded → {weather_flat.count()} rows")
            return weather_flat

    # ----------------------
    # DATA INTEGRATION
    # ----------------------
    def integrate_datasets(self, trips_df, zones_df, weather_df):
        logger.info("Integrating trips + zones + weather...")
        # Join trips with zones on PULocationID
        trips_df = trips_df.join(
            zones_df,
            trips_df["PULocationID"] == zones_df["LocationID"],
            "left_outer"
        ).select(
            trips_df["*"],
            zones_df["Borough"].alias("pickup_borough")
        )
        # Create pickup date column
        trips_df = trips_df.withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime")
        )
        # Prepare weather
        weather_df = self.prepare_weather(weather_df)
        # Join on date
        final_df = trips_df.join(
            weather_df,
            trips_df.pickup_date == weather_df.date,
            how="left_outer"
        ).drop("date")
        logger.info(f"Integrated dataset: {final_df.count()} rows")
        return final_df

    # ----------------------
    # AGGREGATIONS
    # ----------------------
    def aggregate_data(self, df):
        logger.info("Creating aggregated datasets...")
        daily_agg = df.groupBy(
            F.date_trunc("day", "tpep_pickup_datetime").alias("pickup_date"),
            "pickup_borough"
        ).agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("daily_revenue"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("temp_max").alias("avg_temp_max"),
            F.avg("precip").alias("avg_precip")
        )
        hourly_patterns = df.groupBy(
            "pickup_hour",
            "pickup_day_of_week"
        ).agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.stddev("fare_amount").alias("fare_stddev")
        )
        vendor_performance = df.groupBy("VendorID").agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue")
        )
        return {
            "daily_aggregations": daily_agg,
            "hourly_patterns": hourly_patterns,
            "vendor_performance": vendor_performance
        }