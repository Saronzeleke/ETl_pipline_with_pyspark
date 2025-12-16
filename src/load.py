import duckdb
from pathlib import Path
from loguru import logger
from pyspark.sql import DataFrame
from config.settings import config


class DuckDBLoader:
    """
    DuckDB Loader for ETL pipeline.
    Responsibilities:
      - Write Spark DataFrames to DuckDB
      - Export aggregated tables to Parquet
      - Create analytical views
      - List tables
    """

    def __init__(self):
        self.db_path = config.DUCKDB_PATH
        self.conn = self._init_duckdb()
        self._create_output_dirs()

    def _init_duckdb(self):
        """Initialize DuckDB connection and ensure database exists"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(database=str(self.db_path))
        logger.info(f"✅ Initialized DuckDB at {self.db_path}")
        return conn

    def _create_output_dirs(self):
        """Ensure processed folder exists for exports"""
        config.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def write_spark_to_duckdb(self, df: DataFrame, table_name: str) -> int:
        """Write PySpark DataFrame to DuckDB (overwrite)"""
        try:
            # Convert Spark DF to Pandas (DuckDB-friendly)
            pdf = df.toPandas()
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM pdf")
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"✅ Table '{table_name}' written to DuckDB ({row_count} rows)")
            return row_count
        except Exception as e:
            logger.error(f"Failed to write '{table_name}' to DuckDB: {e}")
            raise

    def export_to_parquet(self, table_name: str, export_path: str = None):
        """Export DuckDB table to Parquet"""
        try:
            if not export_path:
                export_path = config.PROCESSED_DATA_DIR / f"{table_name}.parquet"
            else:
                export_path = Path(export_path)
                export_path.parent.mkdir(parents=True, exist_ok=True)

            self.conn.execute(f"COPY {table_name} TO '{str(export_path)}' (FORMAT PARQUET)")
            logger.info(f"✅ Exported '{table_name}' to Parquet → {export_path}")
        except Exception as e:
            logger.error(f"Failed to export '{table_name}' to Parquet: {e}")
            raise

    def create_views(self):
        """Create analytical views in DuckDB"""
        try:
            self.conn.execute("""
                CREATE OR REPLACE VIEW vw_daily_revenue AS
                SELECT pickup_date, SUM(daily_revenue) AS total_revenue
                FROM daily_aggregations
                GROUP BY pickup_date
                ORDER BY pickup_date
            """)
            self.conn.execute("""
                CREATE OR REPLACE VIEW vw_vendor_summary AS
                SELECT VendorID, total_trips, total_revenue
                FROM vendor_performance
            """)
            logger.info("✅ Analytical views created (vw_daily_revenue, vw_vendor_summary)")
        except Exception as e:
            logger.error(f"Failed to create views: {e}")
            raise

    def list_tables(self):
        """Return list of all tables and views in DuckDB"""
        try:
            result = self.conn.execute("SHOW TABLES").fetchall()
            tables = [r[0] for r in result]
            logger.info(f"✅ Current DuckDB tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise

    def close(self):
        """Close DuckDB connection"""
        try:
            self.conn.close()
            logger.info("✅ DuckDB connection closed")
        except Exception as e:
            logger.error(f"Failed to close DuckDB connection: {e}")
