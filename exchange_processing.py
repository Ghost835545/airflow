from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator



from datetime import datetime
import json
import xml.etree.ElementTree as ET
import requests
import xmltodict

default_args = {
    'start_date' : datetime(2022, 1, 1)
}

def _processing_data(ti):
    l = []
    data = ti.xcom_pull(task_ids=['extracting_data'])
    for index in data[0]['ValCurs']['Valute']:
        l.append(
        {
            "Valute ID": index['@ID'],
            "NumCode": index['NumCode'],
            "CharCode": index['CharCode'],
            "Nominal": index['Nominal'],
            "Name": index['Name'],
            "Value": index['Value'],
            "Date": data[0]['ValCurs']['@Date'],

        }
        )
        print(l[0])





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
        headers={"Content-Type": "application/xml"},
        response_filter=lambda response: xmltodict.parse(response.text),
        log_response=True
    )

    processing_data=PythonOperator(
        task_id='processing_data',
        python_callable=_processing_data
    )
is_reosurce_available >> extracting_data >> processing_data

