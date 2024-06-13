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
from markupsafe import soft_unicode





@dag(start_date=datetime(2023, 8, 23), schedule_interval= '*/15 * * * *', catchup=False, render_template_as_native_obj=True, tags=['fz', 'test', 'bq'])
def test8():
 
    def retrieve_url_dynamically(**kwargs): 

        hook = HttpHook(http_conn_id= 'BS_Traffic', method="GET") #established UI connection
        data_from_endpoint = hook.run()
        print(data_from_endpoint.json())
        
        kwargs['ti'].xcom_push(key= "json_data", value = data_from_endpoint.json())

    get_url_task = PythonOperator(
        task_id= 'retrieve_url_task', 
        python_callable = retrieve_url_dynamically,
        provide_context = True,)

    #function to send a post request 
    def upload_to_gcs(**kwargs): 
        data= kwargs['ti'].xcom_pull(task_ids = 'retrieve_url_task', key ="json_data")
        #pd.DataFrame.from_dict(data, orient='index', columns=['value'])
         
        traffic_data = []
        json_object = " "
        
        for features in data['features']:

            properties = features['properties']
            print( properties)
            atributes = {
                "id": properties["id"],
                "name" : properties["name" ],
                "description": properties["description"],
                "trafficSituation": properties["trafficSituation"],
                "DATE_ADDED": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                 }
            json_object+= json.dumps(atributes)
            json_object+= "\n"
            
            
            traffic_data.append(atributes)
        
            
     
        gcs_hook = GCSHook()
        bucket_name = 'bs_traffic_data' #create bucket
        now = datetime.now()
        formated_datetime = now.strftime("%Y-%m-%d_%I-%M-%S")
        print(formated_datetime)
        object_name =f"object_{formated_datetime}"

        gcs_hook.upload(bucket_name= bucket_name, object_name = object_name, data= json_object, mime_type = 'application/json')
        #print(object)
        kwargs['ti'].xcom_push(key= "object_name", value = object_name)
        

    upload_to_gcs_bucket = PythonOperator(
        task_id= 'upload_file_to_gcs_bucket', 
        python_callable =  upload_to_gcs,
        provide_context = True,)

    def upload_to_big_query(**kwargs): 
        object_name = kwargs['ti'].xcom_pull(task_ids = 'upload_file_to_gcs_bucket', key = 'object_name')
        
    
        
        task_3 = GCSToBigQueryOperator(
            task_id = 'upload_to_bg_temp',
            bucket = 'bs_traffic_data',
            source_objects = object_name,
       
            destination_project_dataset_table = 'de-ist-energy-innoportal.city_bs_data.bs_traffic', 
            source_format= 'NEWLINE_DELIMITED_JSON',
            create_disposition="CREATE_IF_NEEDED",
            project_id= 'de-ist-energy-innoportal',
            ignore_unknown_values = True,
            write_disposition='WRITE_APPEND',)

        task_3.execute(kwargs)
    Send_task_to_bq= PythonOperator(

        task_id= 'upload_to_Big_query', 
        python_callable =  upload_to_big_query,
        provide_context = True,)
        



        #bigquery_conn_id = 'de-ist-energy-innoportal',
        #google_cloud_storage_conn_id = 'de-ist-energy-innoportal',
        
        


    get_url_task >> upload_to_gcs_bucket >> Send_task_to_bq
    

test8()