import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

from datetime import datetime

dag = DAG(
    dag_id = "Tiki_batching_flow",
    default_args = {
        "owner": "Kiet Nguyen",
        "start_date": days_ago(1),
    },
    schedule_interval = "30 0 * * *"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

tiki_flow = SparkSubmitOperator(
    task_id="tiki-flow",
    conn_id="spark-conn",
    application="jobs/spark_ETL.py",
    email_on_failure=True,
    email='sirtuankiet@gmail.com',
    conf={
            "spark.jars.packages": "org.postgresql:postgresql:42.6.2",
            # Other Spark configurations
        },
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)



start >> tiki_flow >> end