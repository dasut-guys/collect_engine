from pymongo import MongoClient

class SlackRepository:
    def __init__(self, mongoClient: MongoClient):
        self.mongoClient = mongoClient

    def check_message_id(self, message_id: str) -> bool:
        db = self.mongoClient["slack_app"]
        collection = db["records_info"]

        # "message_id"가 있는지 확인
        if collection.find_one({"message_id": message_id}):
            return True  # 이미 존재하는 message_id
        else:
            return False  # 존재하지 않는 message_id

    def insert_recording_info(
        self,
        message_id,
        message,
        lecture_date,
        video_link,
        password,
        author,
        created_msg_at,
        created_db_at,
    ):
        try:
            # 특정 DB와 Collection 선택
            db = self.mongoClient["slack_app"]  # your_db_name
            collection = db["records_info"]  # collection 이름
            record_data = {
                "message_id": message_id,
                "message": message,
                "lecture_date": lecture_date,  # 강의 날짜
                "video_link": video_link,  # 녹화 파일 날짜
                "password": password,  # 비밀번호
                "author": author,
                "created_msg_at": created_msg_at,
                "created_db_at": created_db_at,  # DB에 추가된 날짜
            }
            # Document 추가
            collection.insert_one(record_data)
        except:
            raise Exception('insert error')

