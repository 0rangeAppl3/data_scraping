import json
import time
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By

from engine.generic_runner import Warehouse
from src.common.utils import convert_string_to_date


class DiceWarehouse(Warehouse):
    def run(self, data_lake, data_warehouse, driver):
        cursor = data_warehouse.cursor()
        collection = data_lake['job_data']
        job_records = collection.find({'ETL': {'$exists': False}, 'website': 'dice'},
                                      no_cursor_timeout=True).batch_size(10)
        job_list = []
        i = 0

        for job_record in job_records:
            i += 1
            print(i, job_record['_id'])
            html_content = job_record['job_page_source']

            driver.execute_script("document.children[0].innerHTML = {}".format(json.dumps(html_content)))
            time.sleep(5)
            try:
                post_tag = driver.find_element(By.TAG_NAME, 'dhi-time-ago')
                posted_date = convert_string_to_date(post_tag.get_attribute("posted-date"))
            except:
                posted_date = datetime.now() - timedelta(days=8)
            try:
                job_title = driver.find_element(By.XPATH, '//*[@id="__next"]/div/main/header/div/div/h1').text
                job_company = driver.find_element(By.XPATH,
                                                  '//*[@id="__next"]/div/main/header/div/div/div[3]/ul/ul[1]/li[1]').text
                job_salary_tag_name = driver.find_element(By.XPATH,
                                                          '//*[@id="__next"]/div/main/header/div/div/div[3]/ul/ul[2]/li[1]/p')
                job_salary = job_salary_tag_name.text if job_salary_tag_name.get_attribute(
                    'data-cy') == 'compensationText' else 'Depends on Experience'
                job_skills_section = driver.find_element(By.TAG_NAME, 'section')
                job_skills_ul_tag = job_skills_section.find_element(By.TAG_NAME, 'ul')
                job_skills_list = job_skills_section.find_elements(By.TAG_NAME,
                                                                   'li') if job_skills_ul_tag.get_attribute(
                    'data-cy') == 'skillsList' else []
                job_skills_text = []

                for skill in job_skills_list:
                    job_skills_text.append(skill.text)
            except:
                job_skills_text = []
                job_salary_tag_name = driver.find_element(By.XPATH,
                                                          '//*[@id="__next"]/div/main/header/div/div/div[2]/ul/ul[2]/li[1]/p')
                job_salary = job_salary_tag_name.text if job_salary_tag_name.get_attribute(
                    'data-cy') == 'compensationText' else 'Depends on Experience'
                job_company = driver.find_element(By.XPATH,
                                                  '//*[@id="__next"]/div/main/header/div/div/div[2]/ul/ul[1]/li[1]').text
                job_title = driver.find_element(By.XPATH, '//*[@id="__next"]/div/main/header/div/div/h1').text

            job_wh_record = {
                'job_skills': ', '.join(job_skills_text),
                'job_salary': job_salary,
                'job_company': job_company,
                'job_title': job_title,
                'job_link': job_record['job_link'],
                'website': 'dice',
                'posted_date': posted_date
            }
            job_list.append(job_wh_record)
            driver.execute_script("document.children[0].innerHTML = ''")

            data_to_upsert = [(str(job_wh_record['job_salary']), str(job_wh_record['job_skills']),
                               str(job_wh_record['job_company']), str(job_wh_record['job_title']),
                               str(job_wh_record['job_link']), str(job_wh_record['website']),
                               job_wh_record['posted_date'])]
            upsert_query = '''
            INSERT INTO job_table (job_salary, job_skills, job_company, job_title, job_link, website, posted_date)
            VALUES %s
            ON CONFLICT (job_link) DO UPDATE
            SET job_salary = EXCLUDED.job_salary,
                job_skills = EXCLUDED.job_skills,
                job_company = EXCLUDED.job_company,
                job_title = EXCLUDED.job_title,
                website = EXCLUDED.website,
                posted_date = EXCLUDED.posted_date;
        '''
            cursor.execute(upsert_query, data_to_upsert)
            data_warehouse.commit()
            collection.update_one({'job_link': job_wh_record['job_link']}, {'$set': {'ETL': True}}, upsert=True)
        print("Done process")
