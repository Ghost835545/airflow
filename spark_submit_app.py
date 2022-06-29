
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime
from airflow.models import DAG


default_args = {
    'start_date' : datetime(2022, 1, 1)
}

with DAG('spark_submit', schedule_interval='@daily',
         default_args=default_args,
         catchup=True) as dag:

    spark_submit_exchange = SparkSubmitOperator(
        task_id="spark_submit_exchange",
        application="/home/ivan/Projects/PySpark/exchange/main.py",
        conn_id="spark_submit"
    )


spark_submit_exchange


