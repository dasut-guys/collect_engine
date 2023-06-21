from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import time
from datetime import datetime

from pymongo import MongoClient


class Zoom:
    def __init__(self):
        pass

    def get_zoom_records(
        self,
        url: str,
        password: str,
        message: str,
        lecture_date: str,
        isHeadless: bool = True,
    ):
        driver = self._run_chrome(url, isHeadless)
        time.sleep(20)

        password_input = driver.find_element(By.XPATH, '//*[@id="passcode"]')

        ActionChains(driver).send_keys_to_element(password_input, password).perform()

        time.sleep(1)

        ok_button = driver.find_element(By.XPATH, '//*[@id="passcode_btn"]')

        ActionChains(driver).click(ok_button).perform()
        time.sleep(10)

        redirect_url = driver.current_url
        cookies = driver.get_cookies()
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

    def _run_chrome(self, url, isHeadless):
        options = webdriver.ChromeOptions()

        if isHeadless:
            options.add_argument("headless")

        driver = webdriver.Chrome("./chromedriver", options=options)

        driver.get(url)

        return driver
