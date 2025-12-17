
import duckdb

con = duckdb.connect('C:/Users/admin/ETl_pipline_with_pyspark/data/output/analytics.db')
con.execute("COPY daily_weather_impact TO 'C:/Users/admin/ETl_pipline_with_pyspark/data/exports/daily_weather_impact.parquet' (FORMAT PARQUET)")
print('✅ Successfully exported daily_weather_impact.parquet')