import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

current_directory = os.getcwd()
task_directory = current_directory + '/airflow/tasks'


args = {
    "owner":"airflow",
    "depends_on_past":False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024,3,29),
    "end_date": datetime(2024,12,31),
    "catchup":False
}

dag = DAG (
    dag_id='formula1_dag',
    default_args=args,
    schedule_interval = '0 0 * * Sun'
    # schedule_interval = '0 0 * * Sun [ $(expr $(date +%W) % 2) -eq 1'
)

extract_data = BashOperator(
    task_id="extract_data",
    bash_command=f"python3 {task_directory}/extract_f1_data.py",
    dag=dag
)

load_to_dev_schema = BashOperator(
    task_id="load_to_dev_schema",
    bash_command=f"python3 {task_directory}/load.py",
    dag=dag
)

dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {task_directory}/dbt && dbt run --profiles-dir .",
        dag=dag
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"cd {task_directory}/dbt && dbt test --profiles-dir .",
    dag=dag
)

upload_csv = BashOperator(
    task_id="upload_csv",
    bash_command=f"python3 {current_directory}/streamlit/blocks/components2.py",
    dag=dag
)



extract_data >> load_to_dev_schema >> dbt_run >> dbt_test >> upload_csv