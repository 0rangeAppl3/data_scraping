from engine.generic_runner import GenericRunner, NonSqlRunner
from engine.spiders.remoteok.ETL_warehouse import RemoteOkWarehouse
from engine.spiders.remoteok.save_job_data import RemoteOkJobDataSaver
from engine.spiders.remoteok.save_search_job_page import RemoteOkSearchJobPageSaver


class RemoteOkRunner(GenericRunner):
    def __init__(self, session):
        super().__init__(session)
        self.save_search_job_page = RemoteOkSearchJobPageSaver()
        self.save_job_data = RemoteOkJobDataSaver()
        self.etl_warehouse = RemoteOkWarehouse()


class NonSqlRemoteOkRunner(NonSqlRunner):
    def __init__(self):
        super().__init__()
        self.save_search_job_page = RemoteOkSearchJobPageSaver()
        self.save_job_data = RemoteOkJobDataSaver()
