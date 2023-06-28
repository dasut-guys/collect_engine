from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils import dates, timezone, xcom

import time
import pendulum
import os 
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

from collectors.slack.slack import Slack
from collectors.zoom import Zoom
from repositories.slack import SlackRepository
from repositories.zoom import ZoomRepository



# load config
load_dotenv(dotenv_path="./config/.env")
config = {
    'mongo' : {
        'uri' :os.environ.get('MONGO_URI')
    },
    'slack' : {
        'token' : os.environ.get('SLACK_API_TOKEN'),
        'd' : os.environ.get('SLACK_D'),
        'd_s' :os.environ.get('SLACK_D_S'),
    },
}


# mongo client init
client = MongoClient(config['mongo']['uri'])

### collectors ###

## slack
slack_collector = Slack(config['slack']['token'], config['slack']['d'], config['slack']['d_s'])

## zoom
zoom_collector = Zoom()

### repositories ###
slack_repo = SlackRepository(client)
zoom_repo = ZoomRepository(client) 


def today_live_records(**context):
        live_records = slack_collector.get_today_live_records()

        # get new records
        new_records = []
        for record in live_records:
            if slack_repo.check_message_id(record["message_id"]):
                continue            
            new_records.append(record)
  
        # insert new records into mongodb
        for record in new_records:
            
            slack_repo.insert_recording_info(
                 record['message_id'], 
                 record['message'], 
                 record['lecture_date'], 
                 record['video_link'], 
                 record['password'], 
                 record['author'], 
                 record['created_msg_at'], 
                 record['created_db_at']
            )

        context['task_instance'].xcom_push(key='new_records', value = new_records)
        return new_records

# def check_new_records(**context):
#      new_records = context['task_instance'].xcom_pull(key='new_records', task_ids='get_records_from_slack_message')
#      return new_records is not []

def get_zoom_records(**context):
    new_records = context['task_instance'].xcom_pull(key='new_records', task_ids='get_new_records_from_slack_message')
    zoom_records = []
    for record in new_records: 
        zoom_record = zoom_collector.get_zoom_records(record['video_link'], record['password'], record['lecture_date'])
        zoom_records.append(zoom_record)
        time.sleep(10)

    context['task_instance'].xcom_push(key='zoom_records', value = zoom_records)
    return zoom_records

# def check_zoom_records(**context):
#     zoom_records = context['task_instance'].xcom_pull(key='zoom_records', task_ids='get_records_from_zoom')
#     return zoom_records is not []
    


def insert_zoom_records(**context):
    zoom_records = context['task_instance'].xcom_pull(key='zoom_records', task_ids='get_zoom_records')
    for zoom_record in zoom_records:
         zoom_repo.insert_recording_info(zoom_record["message"], zoom_record["lecture_date"], zoom_record["result"], zoom_record["created_at"])
    
    print(f"zoom_records {len(zoom_records)}")

local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    "live_chat_records_dag",
    description="실시간 녹화 강의 채팅내역 파이프라인",
    schedule_interval="0 19,20,21,22,23 * * *",
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    catchup=False,
) as dag:
    get_new_records_from_slack_message = PythonOperator(
        task_id="get_new_records_from_slack_message", 
        python_callable=today_live_records,
        provide_context=True
    )

    get_zoom_records = PythonOperator(
        task_id="get_zoom_records", 
        python_callable=get_zoom_records,
        provide_context=True
    )

    insert_zoom_records = PythonOperator(
        task_id="insert_zoom_records", 
        python_callable=insert_zoom_records,
        provide_context=True
    )

    get_new_records_from_slack_message  >> get_zoom_records  >> insert_zoom_records


