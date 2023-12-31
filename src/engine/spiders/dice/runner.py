from engine.generic_runner import GenericRunner, NonSqlRunner
from engine.spiders.dice.ETL_warehouse import DiceWarehouse
from engine.spiders.dice.save_job_data import DiceJobDataSaver
from engine.spiders.dice.save_search_job_page import DiceSearchJobPageSaver


class DiceRunner(GenericRunner):
    def __init__(self, session):
        super().__init__(session)
        self.save_search_job_page = DiceSearchJobPageSaver()
        self.save_job_data = DiceJobDataSaver()
        self.etl_warehouse = DiceWarehouse()


class NonSqlDiceRunner(NonSqlRunner):
    def __init__(self):
        super().__init__()
        self.save_search_job_page = DiceSearchJobPageSaver()
        self.save_job_data = DiceJobDataSaver()
        self.etl_warehouse = DiceWarehouse()
