from financial_data_pipeline.load_bq import load_parquet_to_bigquery, bq_table
from financial_data_pipeline.config import BARS_BASE_DIR

symbol = "TSLA"
input_file = BARS_BASE_DIR / symbol / "bars.parquet"
table_name = "daily_bars"
table_id = bq_table(table_name)

load_parquet_to_bigquery(input_file, table_id, mode="WRITE_TRUNCATE")
