from bs4 import BeautifulSoup
import time
from selenium.webdriver.common.by import By
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Set up Chrome options to run headless
def run(data_lake, driver):
  job_collection = data_lake['job_data']
  page_collection = data_lake['job_page']
  job_list = []
  pages = page_collection.find({'website': 'remoteok'})
  for page in pages:
    # try:
    html_content = page['html_content']
    # Read page source
    driver.execute_script("document.children[0].innerHTML = {}".format(json.dumps(html_content)))

    # Wait for a few seconds to let the page load (adjust as needed)
    time.sleep(1.5)

    try:
      # Get the page source with the dynamically loaded content
      tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
      # tbody = driver.find_element(By.TAG_NAME, 'tbody')
      list_tr= tbody.find_elements(By.TAG_NAME, 'tr')
      i = 0
      for tr in list_tr:
        if tr.get_attribute('data-slug') is not None:
          link=tr.get_attribute('data-slug')
          i += 1
          print('Processing entry ', i)
          page_link= 'https://remoteok.com/remote-jobs/'
          job_link= page_link+ link
          job_record = {
              'job_link': job_link,
              'website': page['website']
          }
          job_list.append(job_record)
    except TimeoutException:
        print(f"Couldn't find tbody for page from website {page['website']}. Moving to the next record.")
        continue

  for job in job_list:
    job_collection.update_one({'job_link': job['job_link']}, {'$set': job}, upsert=True)
  print('saved job data')


