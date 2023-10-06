import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

logging.basicConfig(level=logging.INFO)

# Connect to PostgreSQL database
connection = psycopg2.connect(
    host="localhost",  # replace with your host
    port="5432",  # replace with your port
    database="data_scraping",  # replace with your database name
    user="postgres",  # replace with your user
    password="1212"  # replace with your password
)
cursor = connection.cursor()

# Fetch job_links from PostgreSQL filtered by posted_date
nine_months_ago = datetime.now() - relativedelta(months=9)
cursor.execute("SELECT job_link FROM job_table WHERE posted_date >= %s",(nine_months_ago,))
job_links = cursor.fetchall()

def extract_company_detail(job_link):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    chrome_driver_path = "C:/Program Files/Google/Chrome/Application/chromedriver.exe"
    service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    job_id_number = job_link.split('-')[-1]

    driver.get(job_link)
    logging.info(f"Process link {job_link}")
    wait = WebDriverWait(driver, 20)

    xpath_selectors = [
            f"//tbody/tr[starts-with(@class, 'expand expand-{job_id_number}')][contains(@class, 'active')]/td/div[contains(@class, 'expandContents')]/div[contains(@class, 'description')]/div[contains(@class, 'markdown')]/p",
            f"//tbody/tr[starts-with(@class, 'expand expand-{job_id_number}')][contains(@class, 'active')]/td/div[contains(@class, 'expandContents')]/div[contains(@class, 'description')]/div[contains(@class, 'html')]/p",
            f"//tbody/tr[starts-with(@class, 'expand expand-{job_id_number}')][contains(@class, 'active')]/td/div[contains(@class, 'expandContents')]/div[contains(@class, 'description')]/div[contains(@class, 'html')]/div[3]"
        ]

    for xpath in xpath_selectors:
        try:
            element = wait.until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            company_detail = element.text
            logging.info(f"Selected selector {xpath} successful.")
            logging.info("Added company_detail")
            return company_detail
        except TimeoutException:
            print(f"Element not found or TimeoutException occurred for selector: {xpath}")

    driver.quit()

if __name__ == "__main__":
    for job_link_tuple in job_links:
        job_link = job_link_tuple[0]
        company_detail = extract_company_detail(job_link)
        
        if company_detail:
            # Update the company_detail field in the PostgreSQL table
            cursor.execute(
                "UPDATE job_table SET company_detail = %s WHERE job_link = %s",
                (company_detail, job_link)
            )
            connection.commit()

    # Close the PostgreSQL connection
    cursor.close()
    connection.close()
