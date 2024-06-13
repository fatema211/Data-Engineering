from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow import models
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests

@dag(start_date=datetime(2023, 8, 14), schedule_interval="@hourly", catchup=False, render_template_as_native_obj=True)
def test4():
    def retrieve_url_task(**kwargs): 

        hook = HttpHook(http_conn_id= 'parkhaeuser', method="GET")
        data_from_endpoint = hook.run()
        print(data_from_endpoint.json())
        
        kwargs['ti'].xcom_push(key= "connection_url", value = data_from_endpoint.json())

    get_url_task = PythonOperator(
        task_id= 'retrieve_url_task', 
        python_callable = retrieve_url_task,
        provide_context = True,)
    
    def send_post_requ_task(**kwargs): 
        url= kwargs['ti'].xcom_pull(task_ids = 'retrieve_url_task', key = 'url')
        http_hook = HttpHook(method = 'POST', http_conn_id = 'post_connection')
        response= http_hook.run(endpoint= '/dc25c72a-b6c4-43fc-8a16-5ea6fc869c95', 
        data ="{{ti.xcom_pull(task_ids = 'retrieve_url_task', key=return_value.json())}}")
        print(response.text)

        #print(data)



    post_request_task = PythonOperator( 
        task_id = 'send_request',
        python_callable =  send_post_requ_task,
        provide_context = True, )


    get_url_task >> post_request_task

test4()
