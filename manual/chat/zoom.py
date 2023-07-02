import pymysql
import pandas as pd
from datetime import datetime,timedelta
import base64

def connect_database():
    try:
        return pymysql.connect(host='localhost', user='root', password='root',db='yeardream',charset='utf8mb4')
    except Exception as e:
        raise e
    
create_table_comments = """
CREATE TABLE IF NOT EXISTS comments(
    id VARCHAR(50) PRIMARY KEY, 
    user_id_fk VARCHAR(20), 
    content TEXT NOT NULL, 
    time DATETIME NOT NULL
)
"""

create_table_comments_emoji = """
CREATE TABLE IF NOT EXISTS comments_emojis(
    comments_id_fk VARCHAR(50) NOT NULL , 
    emoji VARCHAR(100) NOT NULL, 
    emoji_count INT NOT NULL
)
"""

create_table_recomments = """
CREATE TABLE IF NOT EXISTS recomments(
    id VARCHAR(50) PRIMARY KEY, 
    comment_id_fk VARCHAR(50) NOT NULL, 
    user_id_fk VARCHAR(20), 
    content TEXT NOT NULL, 
    time DATETIME NOT NULL
)
"""

create_table_recomments_emoji ="""
CREATE TABLE IF NOT EXISTS recomments_emojis(
    recomment_id_fk VARCHAR(50) NOT NULL, 
    emoji VARCHAR(100) NOT NULL, 
    emoji_count INT NOT NULL
)
"""

alter_table_comments = """
ALTER TABLE comments ADD CONSTRAINT fk_comments_users FOREIGN KEY IF NOT EXISTS (user_id_fk) REFERENCES users(id)
"""

alter_table_comments_emojis = """
ALTER TABLE comments_emojis ADD CONSTRAINT fk_commentsEmoji_comments FOREIGN KEY IF NOT EXISTS (comments_id_fk) REFERENCES comments (id)
"""

alter_table_recomments = """
ALTER TABLE recomments ADD CONSTRAINT fk_recomments_comments FOREIGN KEY IF NOT EXISTS (comment_id_fk) REFERENCES comments (id)
"""

alter_table_recomments_two = """
ALTER TABLE recomments ADD CONSTRAINT fk_recomments_user FOREIGN KEY IF NOT EXISTS (user_id_fk) REFERENCES users (id)
"""

alter_table_recomments_emojis = """
ALTER TABLE recomments_emojis ADD CONSTRAINT recommentsEmoji_recomments FOREIGN KEY IF NOT EXISTS (recomment_id_fk) REFERENCES recomments (id)
"""

conn = connect_database()
cursor = conn.cursor()

cursor.execute(create_table_comments)
cursor.execute(create_table_comments_emoji)
cursor.execute(create_table_recomments)
cursor.execute(create_table_recomments_emoji)
cursor.fetchall()
conn.commit()

cursor.execute(alter_table_comments)
cursor.execute(alter_table_comments_emojis)
cursor.execute(alter_table_recomments)
cursor.execute(alter_table_recomments_two)
cursor.execute(alter_table_recomments_emojis)
cursor.fetchall()
conn.commit()


import json
import pymongo
import pandas as pd

from bson.json_util import dumps
from datetime import datetime, timedelta

client = pymongo.MongoClient('mongodb+srv://read:Yeardream23@yeardream.zkm7jta.mongodb.net/?retryWrites=true&w=majority')

db = client["zoom_app"]
collection = db["records"]

start_date = datetime(2023,6,2)
end_date = datetime(2023,7,2)
query = {
    "created_at":{
        "$gte": start_date,
        "$lt": end_date
    }
}
mdb_cursor = collection.find(query)
list_cur = list(mdb_cursor)
json_data = dumps(list_cur, indent=2)


file_list = json.loads(json_data)


def get_user_id(name):
    cursor.execute("SELECT id FROM users WHERE name = %s", (name))
    res = cursor.fetchone()

    if res is None:
        return None
    
    return res[0]

def time_to_korean_date_time(time):
    epoch_time = time / 1000  # 밀리초를 초로 변환
    utc_time = datetime.utcfromtimestamp(epoch_time)  # UTC 시간으로 변환
    korean_time = utc_time + timedelta(hours=9)  # 한국 시간으로 변환 (UTC+9)
    return korean_time

def extract_real_name(nickname):
    positions = ["온라인", "강사", "LM", "오프라인", "운영진"]

    is_reversed = False
    for position in positions:
        if nickname.find(position) == 0:
            is_reversed =True
    
    name = nickname

    if "_" in nickname:
        splited = nickname.split("_")
        if is_reversed:
            name = splited[1]
        else:
            name = splited[0]

    if "_" not in nickname:
        splited = nickname.split(" ")
        if is_reversed:
            name = splited[1]
        else:
            name = splited[0]

    return name


d_comments = []
d_comments_emojis = []
d_recomments = []
d_recomments_emojis = []

for data in file_list:
    xmpplist = data['result']['xmppList']
    for chat in xmpplist:
        if int(chat['time']) != 0:
            name = extract_real_name(chat['senderName'])
            user_id = get_user_id(name)
            korean_time = time_to_korean_date_time(chat['time'])
            d_comments.append({'id' :chat['id'], 'user_id': user_id, 'content': chat['content'], 'time': korean_time})
            
            if 'chatEmojiDetailMap' in chat:
                emoji_data = chat['chatEmojiDetailMap']
                for emoji in emoji_data:
                    # decoded_emoji = base64.b64decode(emoji).decode('utf8mb4')
                    d_comments_emojis.append({'comment_id': chat['id'], 'emoji':emoji, 'emoji_count': emoji_data[emoji]['count']})

            # recomments
            if int(chat['commentTotal'])>0:
                recomments = chat['comments']
                
                for recomment in recomments:
                    name = extract_real_name(recomment['senderName'])
                    user_id = get_user_id(name)
                    korean_time = time_to_korean_date_time(recomment['time'])
                    d_recomments.append({'id':recomment['id'] , 'comment_id': chat['id'], 'user_id': user_id, 'content' : recomment['content'], 'time':korean_time})

                    if 'chatEmojiDetailMap' in recomment:
                        emoji_data = recomment['chatEmojiDetailMap']
                        for emoji in emoji_data:
                            # decoded_emoji = base64.b64decode(emoji).decode('utf8mb4')
                            d_recomments_emojis.append({'recomment_id': recomment['id'], 'emoji':emoji, 'emoji_count': emoji_data[emoji]['count']})

                    

for v in d_comments:
    print(v)
    try:
        cursor.execute("INSERT INTO comments (id, user_id_fk, content, time) VALUES (%s, %s, %s, %s) ", (v['id'], v['user_id'], v['content'], v['time']))
    except:
        pass
    

for v in d_comments_emojis:
    print(v)
    try:
        cursor.execute("INSERT INTO comments_emojis (comments_id_fk, emoji, emoji_count) VALUES(%s, %s ,%s)", 
                   (v['comment_id'], v['emoji'], v['emoji_count']))
    except:
        pass
    
for v in d_recomments:
    print(v)
    try:
        cursor.execute("INSERT INTO recomments (id, comment_id_fk, user_id_fk, content, time) VALUES (%s, %s, %s, %s, %s)",
                   (v['id'], v['comment_id'], v['user_id'], v['content'], v['time']))
    except:
        pass

for v in d_recomments_emojis:
    try:
        cursor.execute("INSERT INTO recomments_emojis (recomment_id_fk, emoji, emoji_count) VALUES(%s, %s ,%s)", 
                   (v['recomment_id'], v['emoji'], v['emoji_count']))
    except:
        pass
    
cursor.fetchall()
conn.commit()

cursor.close()
conn.close()

