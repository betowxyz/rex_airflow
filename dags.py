import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import, timedelta

## Hint: starting airflow
## $ airflow initdb
## $ airflow webserver

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}
dag = DAG(
    dag_id='fluxo_simples',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60)
)