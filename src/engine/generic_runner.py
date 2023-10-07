from typing import Mapping, Any

from psycopg2._psycopg import connection
from pymongo.database import Database
from selenium.webdriver.chrome.webdriver import WebDriver

from engine.session import Session


class SearchJobPageSaver:
    def run(self, data_lake: Database[Mapping[str, Any] | Any], driver: WebDriver):
        raise NotImplementedError("Not implemented for generic runners.")


class JobDataSaver:
    def run(self, data_lake: Database[Mapping[str, Any] | Any], driver: WebDriver):
        raise NotImplementedError("Not implemented for generic runners.")


class Warehouse:
    def run(self, data_lake: Database[Mapping[str, Any] | Any], data_warehouse: connection | connection,
            driver: WebDriver):
        raise NotImplementedError("Not implemented for generic runners.")


class GenericRunner:
    save_search_job_page: SearchJobPageSaver
    save_job_data: JobDataSaver
    etl_warehouse: Warehouse

    def __init__(self, session: Session):
        self.session = session

    def run(self):
        self.save_search_job_page.run(self.session.data_lake, self.session.driver)
        self.save_job_data.run(self.session.data_lake, self.session.driver)
        self.etl_warehouse.run(self.session.data_lake, self.session.data_warehouse, self.session.driver)
