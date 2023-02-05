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
5. Run `prefect agent start  --work-queue "taxi_with_logs"`f
6. After that in gui flows -> select task -> quick run

# 4. Github Storage Block

1. Add to repo directory `git_task` with `web_to_gcs.py`
2. Create flow `4_read_from_repo.py` which task which loading directory from remote and subflow which run loaded flow
3. Run this flow 

# 5. Email or Slack notifications

# 6. Secrets
