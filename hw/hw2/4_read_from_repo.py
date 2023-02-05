from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from prefect.filesystems import GitHub

@flow()
def git_flow():
    github_block = GitHub.load("github")
    github_block.get_directory(from_path="hw/hw2/4", 
                               local_path="clone_repo")

if __name__ == "__main__":
    git_flow()
                            