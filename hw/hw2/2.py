from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    local_path = "/Users/leonidsamorcev/Desktop/zoomcamp/hw/hw2/"
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    return local_path+gcs_path


@task()
def read_with_rows(path: str) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df, df.shape[0]


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gsp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-376311",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_main_flow(months: list = [1], year: int = 2020, color: str = "green"):
    total_rows = 0
    for month in months:
        loaded_rows = etl_gcs_to_bq(month, year, color)
        total_rows += loaded_rows
        print(f"Number of loaded rows: {total_rows}")

@flow()
def etl_gcs_to_bq(month: int = 1, year: int = 2020, color: str = "green"):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df, rows = read_with_rows(path)
    write_bq(df)
    return rows


if __name__ == "__main__":
    etl_gcs_to_bq()
