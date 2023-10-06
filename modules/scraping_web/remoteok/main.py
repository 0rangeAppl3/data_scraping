from . import save_search_job_page
from . import save_job_data
from . import ETL_warehouse
# from 
def run(**kwargs):
  data_lake = kwargs['data_lake']
  data_warehouse = kwargs['data_warehouse']
  driver = kwargs['driver']
  #save_search_job_page.run(data_lake, driver)
  save_job_data.run(data_lake, driver)
  ETL_warehouse.run(data_lake, data_warehouse, driver)