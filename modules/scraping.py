from config.main import Config
from modules.scraping_db import ScrapingDatabase
from pymongo import MongoClient
import psycopg2
from modules.utils import get_proxy
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from modules.utils import get_proxy, run_proxy

def scraping(web_run):
  def wrapper(*args, **kwargs):
    #get config
    Config.get()
    data_lake_config = Config.module.data_lake
    web_driver = Config.module.web_driver
    data_warehouse_config = Config.module.data_warehouse
    #setup data lake
    client = MongoClient(data_lake_config['uri'])
    data_lake = client[data_lake_config['database']]
    #setup data warehouse
    data_warehouse = psycopg2.connect(
      host=data_warehouse_config['host'],
      database=data_warehouse_config['database'],
      user=data_warehouse_config['user'],
      password=data_warehouse_config['password']
    )
    #setup selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    # Set the path to your downloaded Chrome web driver
    chrome_driver_path = web_driver
    # Initialize the Chrome driver
    service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    web_run(data_lake=data_lake, data_warehouse=data_warehouse, chrome_driver_path=chrome_driver_path, driver=driver)

    # get proxy list
    proxy_list = get_proxy()
    # run scraping with list of proxy
    # for proxy in proxy_list:
    #   driver = run_proxy(proxy, service, chrome_options, webdriver, driver)
    #   try:
    #     break
    #   except:
    #     print("Proxy not work")
    driver.quit()
    data_warehouse.close()
    data_warehouse.close()
    client.close()
  return wrapper