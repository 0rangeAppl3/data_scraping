from typing import Any, Mapping

import psycopg2
from psycopg2._psycopg import connection
from pymongo import MongoClient
from pymongo.database import Database
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver

from src.common.config import config
from src.common.utils import get_proxy


class Session:
    driver: WebDriver
    data_warehouse: connection | connection
    data_lake: Database[Mapping[str, Any] | Any]
    client: MongoClient

    def __init__(self):
        # setup data lake
        self.client = MongoClient(config.data_lake.uri)
        self.data_lake = self.client[config.data_lake.database]
        # setup data warehouse
        self.data_warehouse = psycopg2.connect(
            host=config.data_warehouse.host,
            database=config.data_warehouse.database,
            user=config.data_warehouse.user,
            password=config.data_warehouse.password
        )
        # setup selenium
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

        service = Service(executable_path=config.web_driver_path)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # get proxy list
        proxy_list = get_proxy()
        # run scraping with list of proxy
        # for proxy in proxy_list:
        #   driver = run_proxy(proxy, service, chrome_options, webdriver, driver)
        #   try:
        #     break
        #   except:
        #     print("Proxy not work")

    def __del__(self):
        self.driver.quit()
        self.data_warehouse.close()
        self.client.close()
