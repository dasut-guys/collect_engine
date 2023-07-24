import requests
from urllib.parse import urlencode


class SlackAPI:
    def __init__(self, token, d, d_s):
        self.token = token
        self.base_url = "https://yeardream3.slack.com/api"

        self.conversation_history_api = "conversations.history"
        self.coversation_history_params = {
            "slack_route": "",
            "_x_gantry": "true",
            "fp": "d7",
        }

        self.headers = {
            "user_agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
            "Cookie": f"d={d};" + f"d-s={d_s}",
        }

        self.channelMap = {
            "live_zoom_records": "C050VA606UV",
            "CS":"C04T5CDJ51D",
            "ai_math":"C04T5CESFTR",
            "ai_sql":"C04TNCE1X9A",
            "ai_py":"C04TYH5UETT",
            "git":"C05486621TP",
            "ai_py_project":"C055D8K7R6K",
            "ai_mini_data":"C056LP428J2"}

    def get_conversations_history(
        self, channel, start_time_unix, end_time_unix
    ) -> list:
        """
        channel : {'live_zoom_records': "실시간 녹화 강의 채널"}
        """

        payload = {
            "token": f"{self.token}",
            "channel": self.channelMap[channel],
            "oldest": f"{start_time_unix}",
            "latest": f"{end_time_unix}",
        }

        params = self.coversation_history_params
        params["slack_route"] = "T04SPMVG36U"
        query_string = urlencode(params)

        url = self.base_url + "/" + self.conversation_history_api + "?" + query_string
        response = requests.request("POST", url, headers=self.headers, data=payload)
        data_row = response.json()
        return data_row["messages"]
