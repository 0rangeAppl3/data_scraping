from bs4 import BeautifulSoup
import time
from datetime import datetime

def run(data_lake, driver):
  collection = data_lake['job_page']
  page_list = []
  page_index = 0
  while True:
    page_index = page_index + 1
    print(page_index)
    #url = f"https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&page={page_index}&pageSize=100&filters.postedDate=SEVEN&filters.employmentType=FULLTIME&filters.isRemote=true&language=en&eid=S2Q_,Sg_1"
    url = f"https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&page=1&pageSize=100&filters.postedDate=SEVEN&filters.employmentType=FULLTIME&filters.isRemote=true&language=en&eid=S2Q_,Sg_1"
    driver.get(url)
    # Wait for a few seconds to let the page load (adjust as needed)
    time.sleep(20)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    soup_string = str(soup)
    if "Sorry, we were unable to find any results" in soup_string: #page reach to limit
      break
    
    # Get the page source with the dynamically loaded content
    page_source = driver.page_source
    page_list.append({
      'page_index': page_index,
      'html_content': soup_string,
      'website': 'dice',
      'page_link': url,
      'date': datetime.now()
    })
  # save page_source into database
  for page in page_list:
    collection.update_one({'page_link': page['page_link']}, {'$set': page}, upsert=True)


  print('saved search job pages')


