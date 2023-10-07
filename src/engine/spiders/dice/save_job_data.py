import json
import time

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

from engine.generic_runner import JobDataSaver


class DiceJobDataSaver(JobDataSaver):
    # Set up Chrome options to run headless
    def run(self, data_lake, driver):
        job_collection = data_lake['job_data']
        page_collection = data_lake['job_page']
        job_list = []
        pages = page_collection.find({'website': 'dice'})
        for page in pages:
            try:
                html_content = page['html_content']
                # Read page source
                driver.execute_script("document.children[0].innerHTML = {}".format(json.dumps(html_content)))

                # Wait for a few seconds to let the page load (adjust as needed)
                time.sleep(5)

                # Get the page source with the dynamically loaded content
                search_card = driver.find_element(By.TAG_NAME, 'dhi-search-cards-widget')
                hyper_links = search_card.find_elements(By.TAG_NAME, 'h5')
                interation = len(hyper_links)

                for i in range(interation):
                    if hyper_links[i].get_attribute('childElementCount') == '1':
                        job_link = hyper_links[i].find_element(By.TAG_NAME, 'a').get_attribute('href')
                        job_link = job_link.split('?')[0]
                        job_record = {
                            'job_link': job_link,
                            'website': page['website']
                        }
                        job_list.append(job_record)

                driver.execute_script("document.children[0].innerHTML = ''")
            except:
                break

        job_index = 0
        # Extract page_souce of single job
        for job in job_list:
            if job_collection.count_documents({'job_link': job['job_link']}) == 0:  # job does not exist in collection
                job_index += 1
                driver.get(job['job_link'])
                time.sleep(30)
                job_page_source = driver.page_source
                soup = BeautifulSoup(job_page_source, "html.parser")
                soup_string = str(soup)

                job['job_page_source'] = soup_string

        # save page source into database
        for job in job_list:
            job_collection.update_one({'job_link': job['job_link']}, {'$set': job}, upsert=True)

        print('saved job data')
