import pyodbc
from config import Config

class DatabaseConnection:
    def get_db_connection():
        cnxn = pyodbc.connect(f'DRIVER={{{Config.DRIVER}}};SERVER={Config.SERVER};DATABASE={Config.DATABASE};UID={Config.USERNAME};PWD={Config.PASSWORD}')
        return cnxn