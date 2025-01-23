import os
import pandas as pd
from .base import BankReader
import re
from datetime import datetime

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

    def find_matching_column(self, df, possible_names):
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def find_data_start(self, df):
        for idx, row in df.iterrows():
            if any(str(val).strip() == 'Data' for val in row if pd.notna(val)):
                return idx + 1
        return None

    def determine_transaction_type(self, description, value):
        description_upper = description.upper()
        
        # Check type mapping first
        for tipo, keywords in self.type_mapping.items():
            if any(keyword in description_upper for keyword in keywords):
                return tipo
        
        # Default types based on transaction
        if 'PIX' in description_upper:
            return 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
        elif 'TED' in description_upper:
            return 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
        
        return 'CREDITO' if value > 0 else 'DEBITO'

    def process_file(self, filepath, process_id, upload_progress):
        try:
            print(f"Iniciando processamento do arquivo: {filepath}")
            
            df = pd.read_excel(filepath)
            data_start = self.find_data_start(df)
            
            if data_start is None:
                raise ValueError("Header não encontrado")
            
            df = df.iloc[data_start:]
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            df = df.drop(['', 'Saldo'], axis=1)
            
            total_rows = len(df)
            processed_rows = 0
            
            upload_progress[process_id].update({
                'status': 'processing',
                'current': 0,
                'total': total_rows,
                'message': 'Iniciando processamento...'
            })
            
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            try:
                for index, row in df.iterrows():
                    try:
                        if pd.isna(row['Data']):
                            continue
                            
                        date = pd.to_datetime(row['Data'], format='%d/%m/%Y').date()
                        description = str(row['Histórico']).strip()
                        
                        if pd.isna(description) or not description:
                            continue
                            
                        value_str = str(row['Valor']).replace('R$', '').strip()
                        value = float(value_str.replace('.', '').replace(',', '.'))
                        document = str(row['Documento']).strip()
                        
                        transaction_type = self.determine_transaction_type(description, value)
                        
                        cursor.execute('''
                            INSERT INTO transactions 
                            (date, description, value, type, transaction_type, document)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            date.strftime('%Y-%m-%d'),
                            description,
                            value,
                            'receita' if value > 0 else 'despesa',
                            transaction_type,
                            document if document != 'nan' else None
                        ))
                        
                        processed_rows += 1
                        
                        if processed_rows % 10 == 0:
                            upload_progress[process_id].update({
                                'current': processed_rows,
                                'total': total_rows,
                                'message': f'Processando... {processed_rows}/{total_rows}'
                            })
                            conn.commit()

                    except Exception as e:
                        print(f"Erro ao processar linha {index}: {str(e)}")
                        continue

                conn.commit()
                os.remove(filepath)
                
                upload_progress[process_id].update({
                    'status': 'completed',
                    'current': total_rows,
                    'message': f'Concluído: {processed_rows} transações'
                })
                
                return True

            finally:
                conn.close()

        except Exception as e:
            if 'conn' in locals():
                conn.close()
            raise