from collectors.zoom import Zoom
from pymongo import MongoClient

client = MongoClient()


# slack_app, zoom_app DB 모두 created_at 이 오늘 날짜와 같은 게 있는지 확인

# run slack parser

# insert slack data

zoom_app_db = client["zoom_app"]
records_collections = zoom_app_db["records"]

z = Zoom()
# run zoom parser
records = z.get_zoom_records(
    "message",
    "lectureDate",
    False,
)
# insert zoom data
records_collections.insert_one(records)
