from abc import ABC, abstractmethod
import sqlite3
import pandas as pd

class BankReader(ABC):
    def __init__(self):
        self.name = "Base Reader"
        self.batch_size = 20
        self.timeout = 30

    @abstractmethod
    def get_bank_name(self):
        pass

    def get_db_connection(self):
        return sqlite3.connect('instance/financas.db', timeout=self.timeout)

    def parse_date(self, date_str):
        """Common date parsing logic"""
        try:
            # First try DD/MM/YYYY
            return pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce').date()
        except:
            try:
                # Then try flexible parsing with dayfirst=True
                return pd.to_datetime(date_str, dayfirst=True, errors='coerce').date()
            except:
                return None