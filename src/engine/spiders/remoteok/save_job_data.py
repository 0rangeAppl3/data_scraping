import json
import time

import bs4
import pandas as pd
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from common.storage import get_reader
from engine.generic_runner import JobDataSaver
from paths import DICE_PATH, LISTING_CSV_FILENAME, CSV_DIRNAME, HTML_DIRNAME


class RemoteOkJobDataSaver(JobDataSaver):

    # Set up Chrome options to run headless
    def run(self, data_lake, driver):
        job_collection = data_lake["job_data"]
        page_collection = data_lake["job_page"]
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
                list_tr = tbody.find_elements(By.TAG_NAME, 'tr')
                i = 0
                for tr in list_tr:
                    if tr.get_attribute('data-slug') is not None:
                        link = tr.get_attribute('data-slug')
                        i += 1
                        print('Processing entry ', i)
                        page_link = 'https://remoteok.com/remote-jobs/'
                        job_link = page_link + link
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

    # Lightweight, don't need caching
    def non_sql_run(self, date: str):
        date_path = DICE_PATH / date
        html_path = date_path / HTML_DIRNAME
        csv_dir_path = date_path / CSV_DIRNAME
        summary_path = date_path / LISTING_CSV_FILENAME

        # Already done, skip
        if summary_path.exists():
            return

        csvs = list()
        for file in html_path.iterdir():
            offset = file.name.split(".")[0]
            csv_path = csv_dir_path / "{}.csv".format(offset)
            try:
                get_reader(csv_path)
            except FileNotFoundError:
                with file.open('rb') as fr:
                    self._cook_page(fr.read()).to_csv(str(csv_path), index=False)
            csvs.append(pd.read_csv(str(csv_path)))

        # Concat all into a big CSV
        pd.concat(csvs).to_csv(summary_path, index=False)

    def _cook_page(self, html_content):
        soup = bs4.BeautifulSoup(html_content.decode('utf8', errors='ignore'), 'lxml')
        jobs = list()

        job_desc_elements = soup.find_all(".expandContents")
        job_title_elements = soup.find_all("a", itemprop="url")

        # Need these things
        """
        job_link
        job_title
        job_salary
        job_location
        job_skills
        job_company
        website
        job_description
        posted_date
        """
        for job_desc_ele, job_title_ele in zip(job_desc_elements, job_title_elements):
            job_link = "https://remoteok.com" + job_title_ele["href"]
            job_title = job_title_ele.text.strip()
            # Do the rest here

        return pd.DataFrame(jobs)
