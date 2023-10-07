import json
import re
import time

from selenium.webdriver.common.by import By

from engine.generic_runner import Warehouse


def extract_salary(text):
    salary = 'Depend on experience'
    pattern = r'\$\d+k - \$\d+k'
    matches = re.findall(pattern, text)
    if matches:
        salary_range = matches[0]
        return salary_range
    return salary


class RemoteOkWarehouse(Warehouse):
    def run(self, data_lake, data_warehouse, driver):
        cursor = data_warehouse.cursor()
        collection = data_lake['job_page']
        job_records = collection.find({'website': 'remoteok'})
        for page in job_records:
            html_content = page['html_content']
            # Read page source
            driver.execute_script("document.children[0].innerHTML = {}".format(json.dumps(html_content)))
            # Wait for a few seconds to let the page load (adjust as needed)
            time.sleep(3)
            # Get the page source with the dynamically loaded content
            tbody = driver.find_element(By.TAG_NAME, 'tbody')
            list_tr = tbody.find_elements(By.TAG_NAME, 'tr')
            i = 0
            for tr in list_tr:
                if tr.get_attribute('data-slug') is not None:
                    i += 1
                    id = tr.get_attribute('id')

                    # Extract verified_status and closed_status
                    verified_status = None
                    closed_status = None
                    try:
                        tr.find_element(By.XPATH, f'//*[@id="{id}"]/td[2]/span[contains(@class, "verified")]')
                        verified_status = "verified"
                    except:
                        pass

                    try:
                        tr.find_element(By.XPATH, f'//*[@id="{id}"]/td[2]/span[contains(@class, "closed")]')
                        closed_status = "closed"
                    except:
                        pass

                    job_company = ''
                    try:
                        job_company = tr.find_element(By.XPATH, f'//*[@id="{id}"]/td[2]/span[3]/h3').text
                    except:
                        job_company = tr.find_element(By.XPATH, f'//*[@id="{id}"]/td[2]/span/h3').text
                    link = tr.get_attribute('data-slug')
                    id = tr.get_attribute('id')
                    post_tag = tr.find_element(By.TAG_NAME, 'time')
                    posted_date = post_tag.get_attribute("datetime")
                    job_title = tr.find_element(By.XPATH, f'//*[@id="{id}"]/td[2]/a/h2').text
                    salary = 'Depend on experience'
                    location = []
                    job_location = tr.find_elements(By.CLASS_NAME, 'location')
                    for loc in job_location:
                        if '$' not in loc.text:
                            location.append((re.sub(r'[^(a-z|A-Z) ]', '', loc.text)).strip())
                    salary = extract_salary(tr.text)
                    page_link = 'https://remoteok.com/remote-jobs/'
                    job_link = page_link + link
                    skill = []
                    h3 = tr.find_elements(By.TAG_NAME, 'h3')
                    for list in h3:
                        if list.text not in job_company:
                            elementList = list.text
                            skill.append(elementList)
                if tr.get_attribute('data-id') is not None and tr.get_attribute('data-slug') is None:
                    job_description = tr.find_element(By.TAG_NAME, 'td').text
                    job_wh_record = {
                        'job_link': job_link,
                        'job_title': job_title,
                        'job_salary': salary,
                        'job_location': ','.join(location),
                        'job_skills': ','.join(skill),
                        'job_company': job_company,
                        'website': page['website'],
                        'job_description': job_description,
                        'posted_date': posted_date
                    }

                    # Add the verify_status and closed_status if they exist
                    if verified_status:
                        job_wh_record['verify_status'] = verified_status
                    if closed_status:
                        job_wh_record['closed_status'] = closed_status

                    print(job_wh_record)
                    data_to_upsert = [(str(job_wh_record['job_salary']), str(job_wh_record['job_company']),
                                       str(job_wh_record['job_title']), str(job_wh_record['job_link']),
                                       str(job_wh_record['job_location']), str(job_wh_record['job_skills']),
                                       str(job_wh_record['job_description']), str(job_wh_record['website']),
                                       job_wh_record['posted_date'])]
                    upsert_query = '''
            INSERT INTO job_table (job_salary, job_company, job_title, job_link,job_location,job_skills,job_description, website, posted_date)
            VALUES %s
            ON CONFLICT (job_link) DO UPDATE
            SET job_salary = EXCLUDED.job_salary,
                job_company = EXCLUDED.job_company,
                job_title = EXCLUDED.job_title,
                job_location = EXCLUDED.job_location,
                job_skills= EXCLUDED.job_skills,
                job_description= EXCLUDED.job_description,
                website = EXCLUDED.website,
                posted_date = EXCLUDED.posted_date;
            '''
                    cursor.execute(upsert_query, data_to_upsert)
                    data_warehouse.commit()
                    # job_collection.update_one({'job_link': job_wh_record['job_link']}, {'$set': {'ETL': True}}, upsert=True)
