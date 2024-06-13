from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import StringIO
import requests
import pandas as pd 
import json
import logging
import io
from pathlib import Path


file_path_configuration = Path(__file__).parent/ 'configuration.json'
destatis_configuration = json.load(open(file_path_configuration))


with DAG("destatis_data", start_date=datetime(2023, 5, 11, 18), schedule_interval=None, catchup=False, render_template_as_native_obj=True) as dag:
    def transform_to_belvis_format_callable(table_name, description, sachschluessel,index, **kwargs):     
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids="fetch_destatis_data", key='return_value')
        print(data)
        dataframe = pd.DataFrame({"Symbol": [], "Index": [], "Date": [], "Value": []})

        csv_lines = data.split("\n")
        csv_lines = [line for line in csv_lines if ";" in line]
        csv_header_separator = "Januar;Februar"

        index = 0 

        for line in csv_lines:
            index += 1
            if csv_header_separator in line:
                break

        csv_lines = csv_lines[index:]

        specific_table_line = ""


        for line in csv_lines:
            if description in line:
                specific_table_line = line


        specific_table_line = specific_table_line.split(";")[2:]

        month = 1
        current_year = datetime.now().year-1
        for value in specific_table_line:
            date = f"{current_year}-{month:02d}-01"
            dataframe = dataframe._append({"Symbol": table_name, "Index": f"{sachschluessel} {description}", "Date": date, "Value": value}, ignore_index=True)
            month += 1

        csv = dataframe.to_csv(index=False)
        ti.xcom_push(value=csv, key =f"transformbelvis_{index}")

    def send_to_gcs_task(table_name, gcs_prefix, **kwargs):     
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids="fetch_destatis_data", key='return_value')
        if data is not None and isinstance(data, str):
            try:
                #upload to GCS
                gcs_hook = GCSHook()
                gcs_hook.upload(
                    bucket_name='bucket_for_destatis_data',
                    object_name=f'{gcs_prefix}/{table_name}.csv',
                    data = data)

            except Exception as e:
                logging.error(f"error proccessing  data from {table_name}: {e}")
                raise
        else: 
            logging.error(f"Failed to retreive data from {table_name}. Data{data}")
            raise ValueError("No data fetched")
        
    
    row_without_belvis = destatis_configuration[0].copy()
    row_without_belvis.pop("forBelvis")
    row_without_belvis["name"] = row_without_belvis.pop("tablename")

    print(row_without_belvis)

    fetch_data = SimpleHttpOperator(
        task_id= "fetch_destatis_data",
        http_conn_id='Destatis_API', 
        method='GET',
        endpoint='/genesisWS/rest/2020/data/tablefile',
        headers={"Content-Type": "application/json"},
        log_response= True,
        response_check=lambda response: response.ok,
        data=json.dumps(row_without_belvis),)
                
    upload_to_gcs_ve = PythonOperator(
        task_id=f'upload_to_gcs_ve',
        python_callable = send_to_gcs_task,
        op_kwargs={
            'table_name': row_without_belvis['name'],
            'gcs_prefix': row_without_belvis['index'],
        },
        provide_context=True,)

    upload_to_gcs_vi= PythonOperator(
        task_id=f'upload_to_gcs_vi',
        python_callable = send_to_gcs_task,
        op_kwargs={
            'table_name': row_without_belvis['name'],
            'gcs_prefix': row_without_belvis['index'],
        },
        provide_context=True,)


        
    belvis_transforms = []
    i = 0
    for row in destatis_configuration:
        if row ['forBelvis']:
            transform_to_belvis_format = PythonOperator(
                task_id=f'transform_to_belvis_format_{row["sachschluessel"]}',
                python_callable = transform_to_belvis_format_callable,
                op_kwargs={
                    'table_name': row["tablename"],
                    'description': row["index"],
                    "sachschluessel": row["sachschluessel"],
                    "index" : i
                },
                provide_context=True,)
            belvis_transforms.append(transform_to_belvis_format) 
            i+=1
    
    combine_dummy = DummyOperator(task_id="combine_workflow")


fetch_data >> [upload_to_gcs_ve, upload_to_gcs_vi] >> combine_dummy >> belvis_transforms
