import re
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from psycopg2.extras import DictCursor
from selenium.webdriver.chrome.service import Service
data_warehouse = {
  "host": "127.0.0.1",
  "database": "data_scraping",
  "user": "postgres",
  "password": "1212"
}

conn = psycopg2.connect(**data_warehouse)
cur = conn.cursor(cursor_factory=DictCursor)

web_driver = "C:/Program Files/Google/Chrome/Application/chromedriver.exe"
#setup selenium
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
# Set the path to your downloaded Chrome web driver
chrome_driver_path = web_driver
# Initialize the Chrome driver
service = Service(executable_path=chrome_driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

def fetch_elements(driver, by, value):
    try:
        return driver.find_elements(by, value)
    except Exception as e:
        return []

cur.execute("SELECT * FROM public.job_table WHERE website LIKE 'remoteok' AND (company_website IS NULL OR company_website = '')")

rows = cur.fetchall()
print('fetched all rows')

for row in rows:
    job_link = row['job_link']
    # Extracting id_int from the job_link
    matches = re.findall(r"\d+", job_link)
    if matches:
        id_int = matches[0]
    else:
        print(f"Problem encountered with job_link: {job_link}. Couldn't extract id_int.")
        continue

    driver.get(job_link)

    company_page_elements = fetch_elements(driver, By.XPATH, f'//tr[contains(@class, "expand-{id_int}")]/td/div[@class="expandContents"]/div[@class="description"]/div[contains(@class, "company_profile")]/a[1]')
    
    if not company_page_elements:
        print(f"Not found company_page for job_link: {job_link}.")
        continue

    company_page = company_page_elements[0].get_attribute('href')
    print('Processing ', company_page)
    driver.get(company_page)
    company_website_elements = fetch_elements(driver, By.XPATH, '//div[contains(@class, "company-page-info")]/a[1]')
    
    if not company_website_elements:
        print(f"Company website not found for company_page: {company_page}.")
        continue

    company_website = company_website_elements[0].get_attribute('href')
    print('get wwebsite', company_website)
    # Upsert value to the database
    cur.execute("UPDATE public.job_table SET company_website = %s WHERE job_link = %s", (company_website, job_link))
    conn.commit()
    print('upserted to postgres')
driver.quit()
cur.close()
conn.close()
