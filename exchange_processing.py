from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator



from datetime import datetime
import json
import xml.etree.ElementTree as ET


default_args = {
    'start_date' : datetime(2022, 1, 1)
}

def _processing_data(ti):
    data = ti.xcom_pull(task_ids=['extracting_data'])
    return data


with DAG('exchange_processing', schedule_interval='@daily',
         default_args = default_args,
         catchup=False) as dag:

    #проверяем доступны ли источик данных
    is_reosurce_available = HttpSensor(
        task_id='is_resource_available',
        http_conn_id='exch_con',
        endpoint=''
    )

    #извлечение данных из источника
    extracting_data = SimpleHttpOperator(
        task_id='extracting_data',
        http_conn_id='exch_con',
        endpoint='XML_daily.asp',
        method='GET',
        response_filter= lambda response: ET.ElementTree(ET.fromstring(response.text)),
        log_response=True
    )

    processing_data=PythonOperator(
        task_id='processing_data',
        python_callable=_processing_data
    )
is_reosurce_available >> extracting_data >> processing_data

