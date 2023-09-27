from datetime import timedelta
from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# We're hardcoding the project directory value here for the purpose of the demo, but in a production
# environment this would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/opt/airflow/git/dbt-fabric-airflow.git/dbt"

with DAG(
    "dbt-core-fabrics",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["dbt", "Fabrics"],
) as dag:

    dbt_version = BashOperator(
        task_id="dbt_version",
        bash_command="dbt --version",
    )

    dbt_pwd = BashOperator(
        task_id="dbt_debug",
        bash_command="cd dbt && dbt debug",
    )
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd dbt && dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}" ,
    )

    dbt_version >> dbt_run