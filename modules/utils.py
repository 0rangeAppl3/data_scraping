import requests
from datetime import datetime

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

def convert_string_to_date(date_string):
  date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
  return datetime.strptime(date_string, date_format)