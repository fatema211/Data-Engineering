from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow import models
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import json
import pendulum
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

@dag(start_date=datetime(2023, 8, 23), schedule_interval= '*/15 * * * *', catchup=False, render_template_as_native_obj=True,  tags=['fz', 'test', 'bq'])
def load_car_parks():

    def retrieve_url_dynamically(**kwargs): 
        hook = HttpHook(http_conn_id= 'parkhaeuser', method="GET")
        data_from_endpoint = hook.run()
        print(data_from_endpoint.json())
        kwargs['ti'].xcom_push(key= "json_data", value = data_from_endpoint.json())

    get_url_dynamically = PythonOperator(
        task_id= 'retrieve_url', 
        python_callable = retrieve_url_dynamically,
        provide_context = True,)

   
    def upload_to_gcs(**kwargs): 
        data= kwargs['ti'].xcom_pull(task_ids = 'retrieve_url', key ="json_data")
        #pd.DataFrame.from_dict(data, orient='index', columns=['value'])
        parkhaus_data = []
        json_object = " "
        
        for features in data['features']:

            properties = features['properties']
            print( properties)
            atributes = {
                "id": properties["id"],
                "externalId" : properties["externalId" ],
                "name" : properties["name" ],
                "openingState": properties["openingState"],
                "occupancyRate": properties.get("occupancyRate",None),
                "capacity": properties.get("capacity",None),
                "free": properties.get("free",None),
                "title": properties["title"], 
                "DATE_ADDED": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            json_object+= json.dumps(atributes)
            json_object+= "\n"
            
            parkhaus_data.append(atributes)
        
        gcs_hook = GCSHook()
        bucket_name = 'parkhaeuser_bucket'
        now = datetime.now()
        formated_datetime = now.strftime("%Y-%m-%d_%I-%M-%S")
        print(formated_datetime)
        object_name =f"object_{formated_datetime}"

        gcs_hook.upload(bucket_name= bucket_name, object_name = object_name, data= json_object, mime_type = 'application/json',)
        kwargs['ti'].xcom_push(key= "object_name", value = object_name)
        

    upload_file_to_gcs_bucket = PythonOperator(
        task_id= 'upload_file_to_gcs_bucket', 
        python_callable =  upload_to_gcs,
        provide_context = True,)

    def upload_to_big_query(**kwargs): 
        object_name = kwargs['ti'].xcom_pull(task_ids = 'upload_file_to_gcs_bucket', key = 'object_name')
        
        bq_operator = GCSToBigQueryOperator(
            task_id = 'upload_to_bg_temp',
            bucket = 'parkhaeuser_bucket',
            source_objects = object_name,
       
            destination_project_dataset_table = 'de-ist-energy-innoportal.city_bs_data.car_parks', 
            source_format= 'NEWLINE_DELIMITED_JSON',
            create_disposition="CREATE_IF_NEEDED",
            project_id= 'de-ist-energy-innoportal',
            ignore_unknown_values = True,
            write_disposition='WRITE_APPEND',
            )
        
        bq_operator.execute(kwargs)

    upload_file_to_big_query = PythonOperator(
        task_id= 'upload_file_to_big_query', 
        python_callable = upload_to_big_query,
        provide_context = True,)
    
    
    get_url_dynamically >> upload_file_to_gcs_bucket >> upload_file_to_big_query

     
load_car_parks()
