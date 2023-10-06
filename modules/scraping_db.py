from pymongo import MongoClient


class ScrapingDatabase:

  def __init__(self, uri=None, db_name=None, credentials=None) -> None:
    self.uri = uri
    self.db_name = db_name
    self.credentials = credentials
    self.client = None
    self.db = None
    self.active = False
  
  def setup(self):
    if not bool(self.credentials):
      self.client = MongoClient(self.uri)
    else:
      self.client = MongoClient(
        self.uri,
        username=self.credentials['username'],
        password=self.credentials['password'],
        authSource=self.credentials['authSource'],
        authMechanism='SCRAM-SHA-1'
      )
    self.db = self.client.get_database(self.db_name)
    self.active = True
  
  def collection(self, name):
    if not self.active:
      raise Exception('Not initialized')
    return self.db[name]

  def close_connection(self):
    self.client.close()
