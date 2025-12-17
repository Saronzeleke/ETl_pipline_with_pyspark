# ETl_pipline_with_pyspark
Big data analytics and business inteligence
Technology Stack:

Orchestration: Prefect (v2.14.5) with task retries, flow visualization

Distributed Processing: Apache PySpark (v3.5.0) for scalable transformations

Analytical Database: DuckDB (v0.9.2) for high-performance querying

BI & Visualization: Microsoft Power BI Desktop

Data Quality: Loguru for structured logging, PySpark data validation

📁 Repository Structure
ETL_PIPLINE_WITH_PYSPARK/
├── workflow/
│   └── pipeline.py                    # 🎛️ Prefect orchestration DAG
├── src/
│   ├── extract.py                     # 📥 Data download & Spark loading
│   ├── transform.py                   # 🔄 Cleaning, enrichment, aggregation
│   └── load.py                        # 💾 DuckDB loading & Parquet export
├── config/
│   ├── settings.py                    # ⚙️ Centralized configuration
│   └── spark_config.py                # ⚡ Spark session management
├── data/                              # 🗃️ Data directory (auto-generated)
│   ├── raw/                           # Source files (Parquet, CSV, JSON)
│   ├── processed/                     # Transformed datasets
│   └── exports/                       # Final exports for BI consumption
├── requirements.txt                   # 📦 Python dependencies
├── README.md                         # This documentation
└── dashboard_screenshots/            # 📸 Power BI dashboard visuals