import pytz
from datetime import datetime
import re


class SlackParser:
    def __init__(self):
        pass

    def live_records_info(self,raw_data: list):
        result = []
        for message in raw_data:
            message_id = message['client_msg_id']
            # User
            author = message["user"]

            if author != "U04UWJTF53L":
                continue

            # 시간 계산
            today = datetime.now()
            utc_time = datetime.utcfromtimestamp(float(message["ts"]))
            korea_time = utc_time.replace(tzinfo=pytz.utc).astimezone(
                pytz.timezone("Asia/Seoul")
            )

            # msg title, mainsource 가공 나중에 필요하면 사용
            for block in message["blocks"]:
                for element in block["elements"]:
                    if element["type"] == "rich_text_section":
                        for text_element in element["elements"]:
                            if text_element["type"] == "text" and (
                                "강의 녹화본 공유" in text_element["text"]
                                or "강의 녹화본을 공유" in text_element["text"]
                            ):
                                match = re.search(r"\d+/\d+", text_element["text"])
                                if match:
                                    date = match.group()
                                date_obj = datetime.strptime(date, "%m/%d")
                                formatted_date = date_obj.strftime(
                                    "%m-%d"
                                )  # This will be '06-15'

            # Link, password
            for block in message["blocks"]:
                for element in block["elements"]:
                    if "elements" in element:
                        for sub_element in element["elements"]:
                            if "elements" in sub_element:
                                for item in sub_element["elements"]:
                                    if item["type"] == "link":
                                        video_link = item["url"]
                                        # print(f"Link: {item['url']}")
                                    if (
                                        item["type"] == "text"
                                        and "yeardream23!" in item["text"]
                                    ):
                                        password = item["text"]
                                        # print(f"Text: {item['text']}")
            # message
            message = message["text"]
            # lecture_date
            lecture_date = korea_time.strftime("%Y") + "-" + formatted_date
            # video_link
            # password

            # 디비에 추가된 날짜
            created_db_at = today.strftime("%Y-%m-%d")
            # 메시지 작성 시간
            created_msg_at = korea_time.strftime("%Y-%m-%d")

            result.append({
                'message_id':message_id,
                'message':message,
                'lecture_date':lecture_date,
                'video_link':video_link,
                'password':password,
                'author':author,
                'created_msg_at':created_msg_at,
                'created_db_at':created_db_at,
            })
            
        return result
        ## TO-DO
        ## subtype : channel_join 항목 체크해서 데이터에서 빼기
    
