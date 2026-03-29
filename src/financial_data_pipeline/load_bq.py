from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from financial_data_pipeline.config import GOOGLE_CLOUD_PROJECT, BQ_DATASET_ID
from google.api_core.exceptions import NotFound

def bq_table(table_name: str) -> str:
    return f"{GOOGLE_CLOUD_PROJECT}.{BQ_DATASET_ID}.{table_name}"

def load_parquet_to_bigquery(parquet_path: Path, destination_table: str, mode: str="WRITE_APPEND") -> None:
    client = bigquery.Client(project = GOOGLE_CLOUD_PROJECT)

    df = pd.read_parquet(parquet_path)

    job = client.load_table_from_dataframe(
        df,
        destination_table,
        job_config=bigquery.LoadJobConfig(write_disposition=mode),
    )
    job.result()
    print(f"Loaded {len(df)} rows to {destination_table}")
    return None


def truncate_table(target_table: str):
    client = bigquery.Client(project = GOOGLE_CLOUD_PROJECT)
    try:
        client.query(f"TRUNCATE TABLE `{target_table}`").result()
        print(f"Cleared table {target_table}")
    except NotFound:
        print(f"[WARNING] Table {target_table} does not exist yet, skipping truncate...")


