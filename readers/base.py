from abc import ABC, abstractmethod
import sqlite3
import pandas as pd

class BankReader(ABC):
    def __init__(self):
        self.name = "Base Reader"
        self.batch_size = 5  # Reduced
        self.chunk_size = 100  # For Excel reading
        self.timeout = 120  # 2 minutes timeout
        self.commit_interval = 5  # Commit every N rows

    @abstractmethod
    def get_bank_name(self):
        pass
    
    def get_db_connection(self):
        return sqlite3.connect('instance/financas.db', timeout=self.timeout)

    def validate_value(self, value_str):
        """Validate and convert value string to float"""
        try:
            if pd.isna(value_str):
                return None
            clean_value = str(value_str).replace('R$', '').strip()
            if not clean_value:
                return None
            return float(clean_value.replace('.', '').replace(',', '.'))
        except Exception as e:
            print(f"Error validating value '{value_str}': {str(e)}")
            return None

    def parse_date(self, value):
        """Parse date with validation"""
        try:
            if pd.isna(value):
                return None
            return pd.to_datetime(value, dayfirst=True).date()
        except Exception as e:
            print(f"Error parsing date '{value}': {str(e)}")
            return None