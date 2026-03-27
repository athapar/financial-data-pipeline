from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from financial_data_pipeline.config import BQ_PROJECT_ID, BQ_DATASET_ID

def bq_table(table_name: str) -> str:
    return f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

def load_parquet_to_bigquery(parquet_path: Path, destination_table: str, mode: str="WRITE_APPEND") -> None:
    client = bigquery.Client(project = BQ_PROJECT_ID)
    df = pd.read_parquet(parquet_path)

    job = client.load_table_from_dataframe(
        df,
        destination_table,
        job_config=bigquery.LoadJobConfig(write_disposition=mode),
    )
    job.result()
    print(f"Loaded {len(df)} rows to {destination_table}")
    return None

