import time
import pendulum
import os
import pymysql
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

from collectors.slack.slack import Slack
from collectors.zoom import Zoom
from repositories.slack import SlackRepository
from repositories.zoom import ZoomRepository
from repositories.stats import StatsRepository


# load config
load_dotenv(dotenv_path="../config/.env")
config = {
    "mongo": {"uri": os.environ.get("MONGO_URI")},
    "slack": {
        "token": os.environ.get("SLACK_API_TOKEN"),
        "d": os.environ.get("SLACK_D"),
        "d_s": os.environ.get("SLACK_D_S"),
    },
    "mysql": {
        "host": os.environ.get("MYSQL_HOST"),
        "user": os.environ.get("MYSQL_USER"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "db": os.environ.get("MYSQL_DB"),
    },
}


print(config)


# mongo client init
client = MongoClient(config["mongo"]["uri"])
mysql_client = pymysql.connect(
    host=config["mysql"]["host"],
    user=config["mysql"]["user"],
    password=config["mysql"]["password"],
    db=config["mysql"]["db"],
    charset="utf8mb4",
    ssl={"ca": "/etc/ssl/cert.pem"},
)
### collectors ###

## slack
slack_collector = Slack(
    config["slack"]["token"], config["slack"]["d"], config["slack"]["d_s"]
)

## zoom
zoom_collector = Zoom()

### repositories ###
slack_repo = SlackRepository(client)
zoom_repo = ZoomRepository(client)
stats_repo = StatsRepository(mysql_client)


def today_live_records():
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
            record["message_id"],
            record["message"],
            record["lecture_date"],
            record["video_link"],
            record["password"],
            record["author"],
            record["created_msg_at"],
            record["created_db_at"],
        )

    return new_records


def get_zoom_records(new_records):
    zoom_records = []
    for record in new_records:
        zoom_record = zoom_collector.get_zoom_records(
            record["video_link"],
            record["password"],
            record["message"],
            record["lecture_date"],
            True,
        )
        zoom_records.append(zoom_record)
        time.sleep(10)

    return zoom_records


def insert_zoom_records(zoom_records):
    for zoom_record in zoom_records:
        zoom_repo.insert_recording_info(
            zoom_record["message"],
            zoom_record["lecture_date"],
            zoom_record["result"],
            zoom_record["created_at"],
        )

    print(f"zoom_records {len(zoom_records)}")
    return zoom_records


def insert_zoom_chats(zoom_records):
    chats = zoom_collector.pasre_zoom_chats(zoom_records)
    print(chats)
    ## name -> user_id
    for comment in chats["comments"]:
        user_id = stats_repo.get_user_id(comment["name"])
        comment["user_id"] = user_id

    for recomment in chats["recomments"]:
        user_id = stats_repo.get_user_id(recomment["name"])
        recomment["user_id"] = user_id

    for comment in chats["comments"]:
        try:
            stats_repo.insert_comments(
                comment["id"], comment["user_id"], comment["content"], comment["time"]
            )
        except:
            pass

    for comment_emoji in chats["comments_emojis"]:
        try:
            stats_repo.insert_comments_emojis(
                comment_emoji["comment_id"],
                comment_emoji["emoji"],
                comment_emoji["emoji_count"],
            )

        except:
            pass

    for recomment in chats["recomments"]:
        try:
            stats_repo.insert_recomments(
                recomment["id"],
                recomment["comment_id"],
                recomment["user_id"],
                recomment["content"],
                recomment["time"],
            )
        except:
            pass

    for recomment_emoji in chats["recomments_emojis"]:
        try:
            stats_repo.insert_recomments_emojis(
                recomment_emoji["recomment_id"],
                recomment_emoji["emoji"],
                recomment_emoji["emoji_count"],
            )

        except:
            pass


new_records = today_live_records()
print(new_records)
z = get_zoom_records(new_records)
print(z)
y = insert_zoom_records(z)
insert_zoom_chats(y)
mysql_client.close()
