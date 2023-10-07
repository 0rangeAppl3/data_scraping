from typing import Mapping, Any

from psycopg2 import connection
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

    def __init__(self):
        raise NotImplementedError("Not implemented for generic runners.")

    def run(self, session: Session):
        self.save_search_job_page.run(session.data_lake, session.driver)
        self.save_job_data.run(session.data_lake, session.driver)
        self.etl_warehouse.run(session.data_lake, session.data_warehouse, session.driver)
