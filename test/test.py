import os
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

os.chdir("../src")

from common.config import config

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

service = Service(executable_path=config.web_driver_path)
driver = webdriver.Chrome(options=chrome_options, service=service)

driver.get('http://www.google.com/')

time.sleep(5)  # Let the user actually see something!

search_box = driver.find_element_by_name('q')

search_box.send_keys('ChromeDriver')

search_box.submit()

time.sleep(5)  # Let the user actually see something!

driver.quit()
