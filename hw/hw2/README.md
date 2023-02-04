# 1. Load January 2020 data

# 2. Scheduling with Cron

# 3. Loading data to BigQuery

prefect deployment build ./2.py:etl_main_flow -n "Parameterized ETL" 

prefect deployment apply etl_main_flow-deployment.yaml

prefect agent start  --work-queue "taxi_with_logs"

after that in gui flows -> select task -> quick run

# 4. Github Storage Block

# 5. Email or Slack notifications

# 6. Secrets