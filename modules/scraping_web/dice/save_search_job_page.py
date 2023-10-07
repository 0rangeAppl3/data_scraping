from bs4 import BeautifulSoup
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from modules.utils import get_page_number

def run(data_lake, driver):
  collection = data_lake['job_page']
  page_list = []
  page_index = 0
  
  while True:
    page_index = page_index + 1
    print("Processing page ", page_index)
    url = f"https://www.dice.com/jobs/q-Remote-jobs?page={page_index}"
    driver.get(url)
    
    # Start the timer to measure wait time
    start_time = time.time()
    preload_time = 0.5
    try:
      # Wait for the 15th div element with class "job-details" to appear
      element_present = EC.presence_of_element_located((By.ID, 'job-card_0'))
      WebDriverWait(driver, preload_time).until(element_present)
      
      # End the timer and print the duration
      end_time = time.time()
      wait_time = end_time - start_time
      print(f"Time taken for page_index {page_index} to load the 19th job-details div: {wait_time:.2f} seconds")
      
      navigator = driver.find_element(By.XPATH, "/html/body/dhi-job-search-jcl/div/dhi-seds-pagination")

      
      soup = BeautifulSoup(driver.page_source, "html.parser")
      soup_string = str(soup)
    
      page_list.append({
        'page_index': page_index,
        'html_content': soup_string,
        'website': 'dice',
        'page_link': url,
        'date': datetime.now()
      })

      first_page, last_page = get_page_number(navigator.text)
      if first_page - last_page == 0:
        print("Reach to limit page")
        break
    except:
        print(f"Failed to load the 19th job-details div for page_index {page_index} within {preload_time} seconds.")
        continue  # This ensures that even if an error occurs, the loop will continue for the next page_index
  # save page_source into database
  for page in page_list:
      print('Now processing page link: ', page['page_link'])
      collection.update_one({'page_link': page['page_link']}, {'$set': page}, upsert=True)

  print('saved search job pages')
