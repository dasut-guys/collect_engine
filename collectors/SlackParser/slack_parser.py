from datetime import datetime
import pytz
import json
import re
import requests
import time
from Key_data import token,d, d_s, db_url
from url import record_url
from pushmongo import mongo
from check_slack_mongo import check_mongo

# 현재 시간을 가져옴
current_time = datetime.now()

# 당일 00시로 변경
start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

# 유닉스 시간 계산
start_time_unix = int(time.mktime(start_of_day.timetuple()))

end_time_unix = int(time.time())


payload = {
    "token": f"{token}",
    "channel": "C050VA606UV",
    "oldest" : f"{start_time_unix}",
    "latest": f"{end_time_unix}"
}

headers = {
    'User-Agent': '"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"',
    'Cookie': f'd={d};'+f'd-s={d_s}'
}
response = requests.request("POST", record_url, headers=headers, data=payload)

data_row = response.json()

# 응답에서 필요한 정보 추출
messages = data_row['messages']

n=0

for i in range(500):
    try:
        # message_id
        message_id = messages[i]['client_msg_id']
        
        #check_message_id()
        c = check_mongo()
        check_id = c.message_id(message_id)
        if check_id == True:
            print("이미 존재하는 데이터 입니다. ")
            n += 1
            continue
        
        # User
        author = messages[i]['user']
        
        # check_user()
        if author != "U04UWJTF53L":
            print("관리자의 글이 아닙니다.")
            n += 1
            continue
        
        # 시간 계산
        today = datetime.now()
        utc_time = datetime.utcfromtimestamp(float(messages[i]['ts']))
        korea_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Seoul'))
        
        # msg title, mainsource 가공 나중에 필요하면 사용
        for block in messages[i]["blocks"]:
            for element in block['elements']:
                if element['type'] == 'rich_text_section':
                    for text_element in element['elements']:
                        if text_element['type'] == 'text' and ('강의 녹화본 공유' in text_element['text'] or '강의 녹화본을 공유' in text_element['text']):
                            match = re.search(r'\d+/\d+', text_element['text'])
                            if match:
                                date = match.group() 
                            date_obj = datetime.strptime(date, '%m/%d')
                            formatted_date = date_obj.strftime('%m-%d')  # This will be '06-15'

        # Link, password
        for block in messages[i]['blocks']:
            for element in block['elements']:
                if 'elements' in element:
                    for sub_element in element['elements']:
                        if 'elements' in sub_element:
                            for item in sub_element['elements']:
                                if item['type'] == 'link':
                                    video_link = item['url']
                                    #print(f"Link: {item['url']}")
                                if item['type'] == 'text' and 'yeardream23!' in item['text']:
                                    password = item['text']
                                    #print(f"Text: {item['text']}")
        # message 
        message = messages[i]['text']
        # lecture_date
        lecture_date = korea_time.strftime("%Y")+"-"+formatted_date
        # video_link
        # password

        # 디비에 추가된 날짜
        created_db_at = today.strftime("%Y-%m-%d")
        # 메시지 작성 시간
        created_msg_at = korea_time.strftime("%Y-%m-%d")
        
        # MongoDB 연결
        d = mongo()
        d.recording_mongo(db_url,message_id,message,lecture_date,video_link,password,author,created_msg_at,created_db_at)
        
    except IndexError as e:
        print("IndexError:",str(e))#에러 로그 남기는 곳
        if str(e) == "list index out of range":
            print("더 이상 저장할 메시지가 없습니다.")
            print(f"총 {i-n}개의 글이 저장되었습니다.")
            break