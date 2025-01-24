import os
from .base import BankReader
from read_excel import process_excel_file

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.type_mapping = {
            'PIX RECEBIDO': ['PIX RECEBIDO'],
            'PIX ENVIADO': ['PIX ENVIADO'],
            'TED RECEBIDA': ['TED RECEBIDA', 'TED CREDIT'],
            'TED ENVIADA': ['TED ENVIADA', 'TED DEBIT'],
            'PAGAMENTO': ['PAGAMENTO', 'PGTO', 'PAG'],
            'TARIFA': ['TARIFA', 'TAR'],
            'IOF': ['IOF'],
            'RESGATE': ['RESGATE']
        }

    def get_bank_name(self):
        return self.name

    def process_file(self, filepath, process_id, upload_progress):
        """Call common process_file_with_progress logic"""
        from app import process_file_with_progress
        return process_file_with_progress(filepath, process_id)