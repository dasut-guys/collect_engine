from selenium import webdriver

from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
import requests
import time
from datetime import datetime, timedelta


class Zoom:
    def __init__(self):
        pass

    def get_zoom_records(
        self,
        url: str,
        password: str,
        message: str,
        lecture_date: str,
    ):
        try:
            fire_opts = webdriver.FirefoxOptions()
            driver = webdriver.Remote(
                command_executor="http://remote_selenium:4444", options=fire_opts
            )
            driver.get(url)
            time.sleep(30)

            password_input = driver.find_element(By.XPATH, '//*[@id="passcode"]')
            ActionChains(driver).send_keys_to_element(
                password_input, password
            ).perform()

            time.sleep(1)

            ok_button = driver.find_element(By.XPATH, '//*[@id="passcode_btn"]')
            ActionChains(driver).click(ok_button).perform()
            time.sleep(60)

            redirect_url = driver.current_url
            cookies = driver.get_cookies()

            driver.quit()
            new_cookies = {}
            for cookie in cookies:
                new_cookies[cookie["name"]] = cookie["value"]

            response = requests.get(redirect_url, cookies=new_cookies)
  
            newString = (
                response.text.split("window.recordingMobilePlayData")[1]
                .split("fileId")[1]
                .split("isLogin")[0]
            )

            code = newString[3:-3]
            finalUrl = (
                "https://zoom.us/nws/recording/1.0/play/info/"
                + code
                + "?canPlayFromShare=true&from=share_recording_detail&continueMode=true&componentName=rec-play&"
                + "originRequestUrl="
                + url
                + "&originDomain=zoom.us"
            )

            res = requests.get(finalUrl, cookies=new_cookies)
            result = res.json()["result"]
            current_time = datetime.now()

            data = {
                "message": message,
                "lecture_date": lecture_date,
                "result": result,
                "created_at": current_time,
            }

            return data
        except:
            driver.quit()

    def pasre_zoom_chats(self, data):
        """
        data : zoom_records의 result에 해당하는 부분을 넣어주기.
        """
        d_comments = []
        d_comments_emojis = []
        d_recomments = []
        d_recomments_emojis = []

        for d in data:
            xmpplist = d["result"]["xmppList"]
            for chat in xmpplist:
                if int(chat["time"]) == 0:
                    continue

                # comments
                name = self._extract_real_name(chat["senderName"])
                korean_time = self._time_to_korean_date_time(chat["time"])
                d_comments.append(
                    {
                        "id": chat["id"],
                        "name": name,
                        "content": chat["content"],
                        "time": korean_time,
                    }
                )

                # comments_emojis
                if "chatEmojiDetailMap" in chat:
                    emoji_data = chat["chatEmojiDetailMap"]
                    for emoji in emoji_data:
                        d_comments_emojis.append(
                            {
                                "comment_id": chat["id"],
                                "emoji": emoji,
                                "emoji_count": emoji_data[emoji]["count"],
                            }
                        )

                # recomments
                if int(chat["commentTotal"]) <= 0:
                    continue

                recomments = chat["comments"]

                for recomment in recomments:
                    name = self._extract_real_name(recomment["senderName"])
                    korean_time = self._time_to_korean_date_time(recomment["time"])
                    d_recomments.append(
                        {
                            "id": recomment["id"],
                            "name": name,
                            "comment_id": chat["id"],
                            "content": recomment["content"],
                            "time": korean_time,
                        }
                    )

                    # recomments_emojis
                    if "chatEmojiDetailMap" in recomment:
                        emoji_data = recomment["chatEmojiDetailMap"]
                        for emoji in emoji_data:
                            d_recomments_emojis.append(
                                {
                                    "recomment_id": recomment["id"],
                                    "emoji": emoji,
                                    "emoji_count": emoji_data[emoji]["count"],
                                }
                            )

            return {
                "comments": d_comments,
                "comments_emojis": d_comments_emojis,
                "recomments": d_recomments,
                "recomments_emojis": d_recomments_emojis,
            }

    def _time_to_korean_date_time(self, time):
        epoch_time = time / 1000  # 밀리초를 초로 변환
        utc_time = datetime.utcfromtimestamp(epoch_time)  # UTC 시간으로 변환
        korean_time = utc_time + timedelta(hours=9)  # 한국 시간으로 변환 (UTC+9)
        return korean_time

    def _extract_real_name(self, nickname):
        positions = ["온라인", "강사", "LM", "오프라인", "운영진"]

        is_reversed = False
        for position in positions:
            if nickname.find(position) == 0:
                is_reversed = True

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
