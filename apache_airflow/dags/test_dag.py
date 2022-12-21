from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream

from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure' : True,
    'email_on_retry' : True,
    'email': 'admin@astro.io'
}

def downloading_data(ti, **kwargs):
    #print(kwargs)
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')
    
    #return 42
    ti.xcom_push(key='my_key', value=43)

def _checking_data(ti): # taking data from downloading_data
    my_xcom = ti.xcom_pull(key='my_key', task_ids=['downloading_data'])
    print(my_xcom)

def _failure(context):
    print("On callback failure")
    print(context)


with DAG(
    dag_id='test1_dag', 
    start_date=days_ago(3),
    schedule_interval= "@daily",
    default_args=default_args,
    catchup=False , # backfilling process - execute non-triggered dag runs 
    max_active_runs=5 

    ) as dag:

    downloading_data = PythonOperator(
        task_id = 'downloading_data',
        python_callable = downloading_data,
        #op_kwargs = {'my_param': 34} # send our own param to python func
    )

    checking_data = PythonOperator(
        task_id = 'checking_data',
        python_callable= _checking_data
    )

    waiting_for_data = FileSensor(
        task_id = 'waiting_for_data',
        fs_conn_id='fs_default', #id of connection - create through UI
        filepath = 'my_file.txt',
        #poke_interval = 15     #each 15 sec check the condition
    )

    processing_data = BashOperator(
        task_id = 'processing_data',
        bash_command='exit 0',
        on_failure_callback = _failure
    )


    ###### all are sequatial 
    # downloading_data.set_downstream(waiting_for_data)
    # waiting_for_data.set_downstream(processing_data)

    #processing_data.set_upstream(waiting_for_data)
    #waiting_for_data.set_upstream(downloading_data)

    #downloading_data >> waiting_for_data >> processing_data
    chain(downloading_data,checking_data,  waiting_for_data ,processing_data)


    ##### 2 parallel
    #downloading_data >> [ waiting_for_data, processing_data ]

    #### cross dependancies
    #cross_downstream([downloading_data, checking_data], [waiting_for_data, processing_data])
