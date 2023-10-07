import re
import time
from datetime import datetime

import requests
from selenium.webdriver.common.by import By


def _login_with_cookie(driver, cookie):
    driver.get("https://www.linkedin.com/login")
    driver.maximize_window()
    driver.add_cookie({
        "name": "li_at",
        "value": cookie
    })


def extract_number_from_string(input_string):
    number = re.search(r'\d+', input_string)
    if number:
        return int(number.group())
    else:
        return None


def scroll_website(driver, times):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)
    for i in range(times):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)


def click_and_scroll(driver, times, x_path):
    for i in range(times):
        button = driver.find_element(By.XPATH, x_path)
        driver.execute_script("arguments[0].click();", button)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)


def scraper_api(job_link, session_number):
    payload = {'api_key': '4d9e618864d517b327e5a25fb3c427d3', 'url': str(job_link),
               'session_number': int(session_number)}
    r = requests.get('https://api.scraperapi.com/', params=payload)
    time.sleep(10)
    return r.content


def get_proxy():
    url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
    response = requests.request("GET", url)
    proxy_list = response.text.split("\r\n")
    return proxy_list


def run_proxy(proxy, service, chrome_options, webdriver, driver):
    try:
        driver.quit()
    except:
        pass
    chrome_options.add_argument(f"--proxy-server={proxy}")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def get_page_number(text):
    pattern = r'\d+'
    matches = re.findall(pattern, text)

    if len(matches) >= 2:
        first_page, last_page = map(int, matches[:2])
        return first_page, last_page
    return None


def convert_string_to_date(date_string):
    if len(date_string) < 21:
        return datetime.strptime(date_string, "%Y-%m-%d")
    if len(date_string) == 21:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
