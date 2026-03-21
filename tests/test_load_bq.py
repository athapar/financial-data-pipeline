from financial_data_pipeline.load_bq import load_parquet_to_bigquery
from pathlib import Path
symbol = "TSLA"
bars_dir = Path(r"C:\Users\armaa\OneDrive\Documents\GitFiles\financial-data-pipeline-project\data\raw\polygon\bars")
input_file = bars_dir / symbol / "bars.parquet"

table_id = "market-data-pipeline-490602.marketdatapipeline.daily_bars"
project_id = "market-data-pipeline-490602"
load_parquet_to_bigquery(input_file, table_id, project_id, symbol)
