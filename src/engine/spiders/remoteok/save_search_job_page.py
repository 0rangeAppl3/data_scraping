import time
from datetime import datetime

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from engine.generic_runner import SearchJobPageSaver


def has_new_content_loaded(driver, last_element):
    """Check if new content has loaded by comparing position of the last element."""
    try:
        # Wait for new content to appear after the last known element
        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.XPATH, f"//*[position() > {last_element.get_attribute('xpath')}]")
                                           ))
        return True
    except TimeoutException:
        return False


class RemoteOkSearchJobPageSaver(SearchJobPageSaver):
    def run(self, data_lake, driver):
        collection = data_lake['job_page']

        urls = [
            # "https://remoteok.com/remote-jobs",
            # "https://remoteok.com/?benefits=distributed_team",
            # "https://remoteok.com/remote-engineer-jobs",
            # "https://remoteok.com/remote-exec-jobs",
            # "https://remoteok.com/remote-senior-jobs",
            # "https://remoteok.com/remote-dev-jobs",
            # "https://remoteok.com/remote-finance-jobs",
            # "https://remoteok.com/remote-sys-admin-jobs",
            # "https://remoteok.com/remote-javascript-jobs",
            # "https://remoteok.com/remote-backend-jobs",
            # "https://remoteok.com/remote-golang-jobs",
            # "https://remoteok.com/remote-cloud-jobs",
            # "https://remoteok.com/remote-medical-jobs",
            # "https://remoteok.com/remote-front-end-jobs",
            # "https://remoteok.com/remote-full-stack-jobs",
            # "https://remoteok.com/remote-ops-jobs",
            # "https://remoteok.com/remote-design-jobs",
            # "https://remoteok.com/remote-react-jobs",
            # "https://remoteok.com/remote-infosec-jobs",
            # "https://remoteok.com/remote-mobile-jobs",
            # "https://remoteok.com/remote-content-writing-jobs",
            # "https://remoteok.com/remote-saas-jobs",
            # "https://remoteok.com/remote-recruiter-jobs",
            # "https://remoteok.com/remote-full-time-jobs",
            # "https://remoteok.com/remote-api-jobs",
            # "https://remoteok.com/remote-sales-jobs",
            # "https://remoteok.com/remote-ruby-jobs",
            # "https://remoteok.com/remote-education-jobs",
            # "https://remoteok.com/remote-devops-jobs",
            # "https://remoteok.com/remote-stats-jobs",
            "https://remoteok.com/remote-python-jobs",
            "https://remoteok.com/remote-node-jobs",
            "https://remoteok.com/remote-english-jobs",
            "https://remoteok.com/remote-non-tech-jobs",
            "https://remoteok.com/remote-quality-insurance-jobs",
            "https://remoteok.com/remote-ecommerce-jobs",
            "https://remoteok.com/remote-teaching-jobs",
            "https://remoteok.com/remote-linux-jobs",
            "https://remoteok.com/remote-java-jobs",
            "https://remoteok.com/remote-crypto-jobs",
            "https://remoteok.com/remote-junior-jobs",
            "https://remoteok.com/remote-git-jobs",
            "https://remoteok.com/remote-legal-jobs",
            "https://remoteok.com/remote-android-jobs",
            "https://remoteok.com/remote-accounting-jobs",
            "https://remoteok.com/remote-admin-jobs",
            "https://remoteok.com/remote-microsoft-jobs",
            "https://remoteok.com/remote-excel-jobs",
            "https://remoteok.com/remote-php-jobs",
            "https://remoteok.com/remote-amazon-jobs",
            "https://remoteok.com/remote-serverless-jobs",
            "https://remoteok.com/remote-css-jobs",
            "https://remoteok.com/remote-software-jobs",
            "https://remoteok.com/remote-analyst-jobs",
            "https://remoteok.com/remote-angular-jobs",
            "https://remoteok.com/remote-ios-jobs",
            "https://remoteok.com/remote-customer-support-jobs",
            "https://remoteok.com/remote-html-jobs",
            "https://remoteok.com/remote-salesforce-jobs",
            "https://remoteok.com/remote-product-designer-jobs",
            "https://remoteok.com/remote-hr-jobs",
            "https://remoteok.com/remote-sql-jobs",
            "https://remoteok.com/remote-c-jobs",
            "https://remoteok.com/remote-web-developer-jobs",
            "https://remoteok.com/remote-nosql-jobs",
            "https://remoteok.com/remote-postgres-jobs",
            "https://remoteok.com/remote-c-plus-plus-jobs",
            "https://remoteok.com/remote-jira-jobs",
            "https://remoteok.com/remote-c-sharp-jobs",
            "https://remoteok.com/remote-apache-jobs",
            "https://remoteok.com/remote-data-science-jobs",
            "https://remoteok.com/remote-virtual-assistant-jobs",
            "https://remoteok.com/remote-react-native-jobs",
            "https://remoteok.com/remote-mongo-jobs",
            "https://remoteok.com/remote-testing-jobs",
            "https://remoteok.com/remote-architecture-jobs",
            "https://remoteok.com/remote-director-jobs",
            "https://remoteok.com/remote-wordpress-jobs",
            "https://remoteok.com/remote-laravel-jobs",
            "https://remoteok.com/remote-elasticsearch-jobs",
            "https://remoteok.com/remote-blockchain-jobs",
            "https://remoteok.com/remote-web3-jobs",
            "https://remoteok.com/remote-docker-jobs",
            "https://remoteok.com/remote-graphql-jobs",
            "https://remoteok.com/remote-architect-jobs",
            "https://remoteok.com/remote-machine-learning-jobs",
            "https://remoteok.com/remote-scala-jobs",
            "https://remoteok.com/remote-web-jobs",
            "https://remoteok.com/remote-objective-c-jobs",
            "https://remoteok.com/remote-vue-jobs"
        ]

        for url in urls:
            page_list = []
            driver.get(url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.8)
            for i in range(25):
                # Get the last element on the page before the scroll
                elements_before_scroll = driver.find_elements(By.CSS_SELECTOR, '*')
                last_element = elements_before_scroll[-1]

                # Scroll down
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")

                # Check if new content has loaded
                if not has_new_content_loaded(driver, last_element):
                    break

                # driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                # time.sleep(0.8)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            soup_string = str(soup)

            # Get the page source with the dynamically loaded content
            # page_source = driver.page_source
            page_list.append({
                'html_content': soup_string,
                'website': 'remoteok',
                'page_link': url,
                'date': datetime.now()
            })
            for page in page_list:
                collection.update_one({'page_link': page['page_link']}, {'$set': page}, upsert=True)
            # save page_source into database
            print(f'saved search job pages for URL: {url}')
        print("saved all job page to db")
