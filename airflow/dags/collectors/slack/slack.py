import time
from datetime import datetime

from .api import SlackAPI
from .parser import SlackParser

class Slack:
    def __init__(self, token, d, d_s):
        self.api = SlackAPI(token,d,d_s)
        self.parser =SlackParser()

    def get_today_live_records(self):
          # 현재 시간을 가져옴
        current_time = datetime.now()
        # 당일 00시로 변경
        start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        # 유닉스 시간 계산
        start_time_unix = int(time.mktime(start_of_day.timetuple()))
        # start_time_unix = 1686384000
        end_time_unix = int(time.time())


        raw_data = self.api.get_conversations_history("live_zoom_records", start_time_unix=start_time_unix, end_time_unix=end_time_unix)
        live_records = self.parser.live_records_info(raw_data)

        return live_records