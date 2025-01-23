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

    def parse_date(self, value):
        """Enhanced date parsing"""
        if pd.isna(value):
            return None
        
        try:
            if isinstance(value, str):
                for fmt in ['%d/%m/%Y', '%Y-%m-%d']:
                    try:
                        return datetime.strptime(value.strip(), fmt).date()
                    except ValueError:
                        continue
            return pd.to_datetime(value, dayfirst=True).date()
        except:
            return None

    def process_batch(self, df, start_idx, end_idx, cursor):
        """Common batch processing"""
        processed = 0
        for idx in range(start_idx, end_idx):
            try:
                yield df.iloc[idx], processed
                processed += 1
            except Exception as e:
                print(f"Error in batch process: {e}")
                continue