from datetime import timedelta
from pendulum import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# We're hardcoding the project directory value here for the purpose of the demo, but in a production
# environment this would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "dbt"

@dag(
    start_date=datetime(2022, 3, 14),
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__,
)
def dbt_run_from_failure():
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run