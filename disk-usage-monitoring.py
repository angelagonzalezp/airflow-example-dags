import psutil
import logging
import jinja2
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import BranchPythonOperator
#from airflow.operators.empty import EmptyOperator  #Airflow>=2.3.0
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'angela',
    'start_date': datetime(2024,12,28,10,0,0),
    'disk_usage_threshold': '{{ var.value.disk_percent_thres }}',
    'mailto': '{{ var.value.admin_mail }}'
}

def disk_usage(**context):
    partitions = psutil.disk_partitions()
    disk_partitions_usage = []
    warn = []
    for partition in partitions:
        usage = {}
        logging.info(f"Getting disk usage for partition {partition.mountpoint}")
        usage[partition.mountpoint] = psutil.disk_usage(partition.mountpoint).percent
        disk_partitions_usage.append(usage)
        if(usage[partition.mountpoint]>=float(context["templates_dict"]["disk_usage_threshold"])):
            logging.info(f"Threshold exceeded.")
            warn.append(usage) 
    task_instance = context["task_instance"]
    task_instance.xcom_push(key="disk_thres_exceeded", value=str(warn))        
    if(len(warn)>0):
        return 'warning_mail'
    else:
        return 'no_warning'

with DAG(dag_id='disk_usage_monitoring', default_args=default_args, schedule_interval="@daily",
        template_undefined=jinja2.Undefined) as dag:
    
    get_partitions_usage = BranchPythonOperator(task_id='get_partitions_usage', python_callable=disk_usage, provide_context=True,
                                                templates_dict=default_args, do_xcom_push=True)
    warning_mail = EmailOperator(task_id='warning_mail', to='{{ dag_run.conf["mailto"] }}', subject='Disk usage warning', 
                                html_content='WARNING! Disk usage exceeded max threshold: {{ ti.xcom_pull(task_ids="get_partitions_usage") }}')
    #no_warning = EmptyOperator(task_id='no_warning')
    no_warning = DummyOperator(task_id='no_warning')
    
get_partitions_usage >> [warning_mail, no_warning]