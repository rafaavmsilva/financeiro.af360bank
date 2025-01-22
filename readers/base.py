from abc import ABC, abstractmethod
import sqlite3

class BankReader(ABC):
    def __init__(self):
        self.name = "Base Reader"
    
    @abstractmethod
    def process_file(self, filepath, process_id, upload_progress):
        pass
    
    @abstractmethod
    def get_bank_name(self):
        pass
    
    def get_db_connection(self):
        return sqlite3.connect('instance/financas.db')