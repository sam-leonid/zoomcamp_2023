from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}.parquet"
    df.to_parquet(Path(path), compression="gzip")
    return path


@task()
def write_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    from prefect.filesystems import GCS
    gcs_block = GCS.load("zoom-gcs")
    gcs_block.save(path)
    # gcs_block = GcsBucket.load("zoom-gcs")
    # gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_parent_flow(months: list = [1], year: int = 2020, color: str = "green"):
    for month in months:
        etl_web_to_gcs(year, month, color)

@flow()
def etl_web_to_gcs(month: int = 11, year: int = 2020, color: str = "green") -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
