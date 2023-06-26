from pymongo import MongoClient


# MongoDB 연결
class mongo:
    def __init__(self):
        pass
    
    def recording_mongo(self,db_url,message_id,message,lecture_date,video_link,password,author,created_msg_at,created_db_at):
        "MongoDB연결/your_mongodb_uri"
        client = MongoClient(db_url)
        
        # 특정 DB와 Collection 선택
        db = client["slack_app"] #your_db_name
        collection = db["records_info"] #collection 이름
        
        record_data = {
            "message_id": message_id,
            "message": message,
            "lecture_date": lecture_date, # 강의 날짜
            "video_link": video_link, # 녹화 파일 날짜
            "password": password, # 비밀번호
            "author": author,
            "created_msg_at": created_msg_at,
            "created_db_at":  created_db_at #DB에 추가된 날짜
        }

        # Document 추가
        collection.insert_one(record_data)
        print("Done!!!!!!!")