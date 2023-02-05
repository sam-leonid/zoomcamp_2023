from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from prefect.filesystems import GitHub

@task()
def git_clone(from_path:str,
             local_path:str) -> None:
    """Clone remote file from github"""
    github_block = GitHub.load("github")
    github_block.get_directory(from_path=from_path, 
                               local_path=local_path)
    return

@task()
def exec_clone_code(from_path:str,
                    file_name:str) -> None:
    from clone_repo.hw.hw2.git_task.web_to_gcs import etl_web_to_gcs
    etl_web_to_gcs()
    

@flow()
def git_flow(from_path:str = "hw/hw2/4",
             local_path:str = "clone_repo",
             file_name:str = "4.py"):
    git_clone(from_path, local_path)
    exec_clone_code(local_path, file_name)
    

if __name__ == "__main__":
    git_flow()
                            