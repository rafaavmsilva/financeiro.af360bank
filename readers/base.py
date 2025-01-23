from abc import ABC, abstractmethod
import sqlite3
import pandas as pd
from datetime import datetime

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

    def validate_value(self, value_str):
        try:
            clean_value = str(value_str).replace('R$', '').strip()
            return float(clean_value.replace('.', '').replace(',', '.'))
        except:
            return None

    def parse_date(self, value):
        if pd.isna(value):
            return None
        try:
            return pd.to_datetime(value, format='%d/%m/%Y').date()
        except:
            try:
                return pd.to_datetime(value, dayfirst=True).date()
            except:
                return None