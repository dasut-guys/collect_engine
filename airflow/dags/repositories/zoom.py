from pymongo import MongoClient

class ZoomRepository:
    def __init__(self, mongoClient: MongoClient):
        self.mongoClient = mongoClient

    def insert_recording_info(
        self,
        message,
        lecture_date,
        result,
        created_at,
    ):
        try:
            # 특정 DB와 Collection 선택
            zoom_app_db = self.mongoClient["zoom_app"]
            records_collections = zoom_app_db["records"]

            data = {
            "message": message,
            "lecture_date": lecture_date,
            "result": result,
            "created_at": created_at,
            }

            # Document 추가
            records_collections.insert_one(data)
        except:
            raise Exception('insert error')

