# 1. Load January 2020 data

1. Change default input data for this task (file renamed to 1.py)
2. Run script with `python 1.py`
3. Show number of rows 

# 2. Scheduling with Cron

1. Rename file to 2.py
2. Run `prefect deployment build 2.py:etl_parent_flow -n etl --cron "0 5 1 * *" -a`

# 3. Loading data to BigQuery

1. Edit `2.py` with delete T from ETL
2. Run `prefect deployment build ./2.py:etl_main_flow -n "Parameterized ETL"`
3. Change params in new file `etl_main_flow-deployment.yaml`
4. Run `prefect deployment apply etl_main_flow-deployment.yaml`
5. Run `prefect agent start  --work-queue "taxi_with_logs"`
6. After that in gui flows -> select task -> quick run

# 4. Github Storage Block

1. Add to repo directory `git_task` with `web_to_gcs.py`
2. Create flow `4_read_from_repo.py` which task which loading directory from remote and subflow which run loaded flow
3. Run this flow 

# 5. Email or Slack notifications


1. Create account on prefect cloud and login with creating API key
2. Add parametrization to flow `4_read_from_repo.py`.
``` 
prefect deployment build ./4_read_from_repo.py:git_flow -n "Parameterized gitflow"
prefect deployment apply git_flow-deployment.yaml
```

3. Create Block GitHub
4. Create Block Email
5. Create Block GCS which have other type:
```
from prefect.filesystems import GCS
gcs_block = GCS.load("zoom-gcs")
gcs_block.put_directory(local_path=path, to_path=path)
```

6. Add Automatitions `send_email_success` for send email while Completed flow and `send_slack_success` for send mail to slack while Completed flow.
7. Run flow `prefect agent start  --work-queue "git-flow"`

# 6. Secrets

Add secret and show number of symbols