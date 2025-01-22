from .base import BankReader
from datetime import datetime
import pandas as pd
import re
import os

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.column_mapping = {
            'data': ['Data', 'DATE', 'DT', 'AGENCIA'],
            'descricao': ['Histórico', 'HISTORIC', 'DESCRIÇÃO', 'DESCRICAO', 'CONTA'],
            'valor': ['Valor', 'VALUE', 'QUANTIA', 'Unnamed: 4']
        }

    def get_bank_name(self):
        return self.name

    def find_matching_column(self, df, possible_names):
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def process_file(self, filepath, process_id, upload_progress):
        try:
            print(f"Iniciando processamento do arquivo: {filepath}")
            
            # Initialize progress tracking
            df = pd.read_excel(filepath)
            total_rows = len(df)
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': total_rows,
                'message': 'Lendo arquivo...'
            }
            
            # Find columns
            data_col = self.find_matching_column(df, self.column_mapping['data'])
            desc_col = self.find_matching_column(df, self.column_mapping['descricao'])
            valor_col = self.find_matching_column(df, self.column_mapping['valor'])
            
            if not all([data_col, desc_col, valor_col]):
                raise ValueError(f"Colunas necessárias não encontradas: {df.columns.tolist()}")
            
            # Process rows
            conn = self.get_db_connection()
            cursor = conn.cursor()
            processed_rows = 0

            for index, row in df.iterrows():
                try:
                    # Update progress
                    upload_progress[process_id].update({
                        'current': index + 1,
                        'message': f'Processando linha {index + 1} de {total_rows}'
                    })
                    
                    # Process data
                    date = pd.to_datetime(row[data_col]).date()
                    description = str(row[desc_col]).strip()
                    value = float(str(row[valor_col]).replace('R$', '').replace('.', '').replace(',', '.'))
                    
                    # Insert transaction
                    cursor.execute('''
                        INSERT INTO transactions (date, description, value, type)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        date.strftime('%Y-%m-%d'),
                        description,
                        value,
                        'receita' if value > 0 else 'despesa'
                    ))
                    
                    processed_rows += 1
                    
                except Exception as e:
                    print(f"Erro na linha {index + 1}: {str(e)}")
                    continue

            # Finalize
            conn.commit()
            conn.close()
            os.remove(filepath)
            
            upload_progress[process_id].update({
                'status': 'completed',
                'current': total_rows,
                'message': f'Concluído: {processed_rows} transações importadas'
            })
            
            return True

        except Exception as e:
            upload_progress[process_id].update({
                'status': 'error',
                'message': f'Erro: {str(e)}'
            })
            if 'conn' in locals():
                conn.close()
            raise