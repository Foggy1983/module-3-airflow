from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

#test-2
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")
for rocket in ("all","falcon1","falcon9","falconheavy"):
    if rocket == "all":
        rocket ==''
    t1 = BashOperator(
        task_id="get_data" + rocket, 
        bash_command="python3 /root/airflow/dags/spacex/load_launches.py -r {{params.rocket}} -y {{ execution_date.year }} -o /var/data", 
        params={"rocket": rocket},
        dag=dag
    )
    if rocket == '':
        rocket == "all"
    t2 = BashOperator(
        task_id="print_data" + rocket, 
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv", 
        params={"rocket": rocket}, # falcon1/falcon9/falconheavy
        dag=dag
    )

t1 >> t2
