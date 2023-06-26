from pymongo import MongoClient
from Key_data import db_url

# MongoDB 연결
class check_mongo:
    def __init__(self):
        pass    
    
    def message_id(self,message_id):
        "MongoDB연결/your_mongodb_uri"
        client = MongoClient(db_url)
        
        # 특정 DB와 Collection 선택
        db = client["slack_app"] #your_db_name
        collection = db["records_info"] #collection 이름
            
        # "message_id"가 있는지 확인
        if collection.find_one({"message_id": message_id}):
            return True   # 이미 존재하는 message_id
        else:
            return False  # 존재하지 않는 message_id
        
