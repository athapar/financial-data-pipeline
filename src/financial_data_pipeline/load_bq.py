from pathlib import Path
import pandas as pd
from google.cloud import bigquery


def load_parquet_to_bigquery(parquet_path: Path, table_id: str, project_id: str, symbol: str, mode="WRITE_APPEND") -> None:
    client = bigquery.Client(project = project_id)
    df = pd.read_parquet(parquet_path)

    df['symbol'] = symbol
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition=mode),
    )
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")

