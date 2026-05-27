from pathlib import Path
from typing import List
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


def batch_load_parquet_to_bigquery(parquet_paths: List[Path], destination_table: str) -> int:
    dfs = []
    for p in parquet_paths:
        if p.exists():
            dfs.append(pd.read_parquet(p))

    if not dfs:
        print(f"No data to load for {destination_table}")
        return 0

    combined = pd.concat(dfs, ignore_index=True)

    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)
    job = client.load_table_from_dataframe(
        combined,
        destination_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(combined)} rows to {destination_table} ({len(dfs)} files)")
    return len(combined)


def truncate_table(target_table: str):
    client = bigquery.Client(project = GOOGLE_CLOUD_PROJECT)
    try:
        client.query(f"TRUNCATE TABLE `{target_table}`").result()
        print(f"Cleared table {target_table}")
    except NotFound:
        print(f"[WARNING] Table {target_table} does not exist yet, skipping truncate...")


