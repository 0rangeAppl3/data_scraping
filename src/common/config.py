import json


class DataLakeConf:
    uri: str
    database: str

    def __init__(self, uri, database):
        self.uri = uri
        self.database = database


class DataWarehouseConf:
    host: str
    database: str
    user: str
    password: str

    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password


class Config:
    data_lake: DataLakeConf
    data_warehouse: DataWarehouseConf
    web_driver_path: str

    def __init__(self, config_path: str):
        with open(config_path, encoding='utf8') as fr:
            obj = json.load(fr)
        self.data_lake = DataLakeConf(
            uri=obj["data_lake"]["uri"],
            database=obj["data_lake"]["database"]
        )
        self.data_warehouse = DataWarehouseConf(
            host=obj["data_warehouse"]["host"],
            database=obj["data_warehouse"]["database"],
            user=obj["data_warehouse"]["user"],
            password=obj["data_warehouse"]["password"]
        )
        self.web_driver_path = obj["web_driver"]


config = Config("static/config.json")
