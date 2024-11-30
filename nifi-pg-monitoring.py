import jinja2
import logging
import json
import time
from utils.nifi_api import *
from datetime import datetime
from pymongo import MongoClient

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'angela',
    'start_date': datetime(2024,11,30,18,0,0),
    'nifi_user': '{{ var.json.nifi_secrets.user  }}',
    'nifi_pwd': '{{ var.json.nifi_secrets.pass  }}',
    'nifi_url': '{{ var.value.nifi_url }}',
    'process_group_id': '{{ var.value.nifi_process_group }}',
    'mongo_url': '{{ var.value.mongo_url }}'
}

def process_group_monitoring(**context):
    url = context["templates_dict"]["nifi_url"]
    api_url = f"{url}/nifi-api/"
    user = context["templates_dict"]["nifi_user"]
    pwd = context["templates_dict"]["nifi_pwd"]
    pg_id = context["templates_dict"]["process_group_id"]
    access_token = get_token(f"{api_url}access/token", user, pwd)
    pg_stats = get_process_group_stats(api_url,access_token, pg_id)
    pg_stats = json.loads(pg_stats)
    metrics = pg_stats["status"]["aggregateSnapshot"]
    metrics["running_processors"] = pg_stats["runningCount"]
    metrics["process_group"] = pg_id
    log_name = "./dags/outputs/pg_" + pg_id + '_' + str(int(time.time())) + ".json"
    logging.info(f"Generating {log_name} file")
    with open(log_name, 'w+') as f:
        json.dump(metrics, f)
    task_instance = context["task_instance"]
    task_instance.xcom_push(key="log_file", value=log_name)
    
def upload_stats_to_mongo(**context):
    ti = context["task_instance"]
    json_file = ti.xcom_pull(task_ids="get_pg_stats", key="log_file")
    mongo_uri = context["templates_dict"]["mongo_url"]
    try:
        client = MongoClient(mongo_uri)
        collection = client['nifi-stats']['process_group_monitoring']
    except Exception as e:
        raise Exception(e)
    with open(json_file) as f:
        data = json.load(f)
        try:
            collection.insert_one(data)
        except Exception as err:
            raise Exception(err)
    client.close()
    return json_file
    

with DAG(dag_id="nifi_pg_monitoring", default_args=default_args, schedule_interval="@hourly",
        template_undefined=jinja2.Undefined) as dag:

    get_pg_stats = PythonOperator(task_id='get_pg_stats', python_callable=process_group_monitoring,
                                templates_dict=default_args, do_xcom_push=True, provide_context=True)
    upload_to_mongo = PythonOperator(task_id='upload_to_mongo', python_callable=upload_stats_to_mongo,
                                templates_dict=default_args, do_xcom_push=True, provide_context=True)
    remove_json_file = BashOperator(task_id='remove_json_file', bash_command='rm -f {{ ti.xcom_pull(task_ids="upload_to_mongo") }}', 
                                    do_xcom_push=True)
    
get_pg_stats >> upload_to_mongo >> remove_json_file