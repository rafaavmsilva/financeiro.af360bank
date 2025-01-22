from .base import BankReader
from datetime import datetime
import pandas as pd
import re
import os

class ItauReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Itaú"
        self.column_mapping = {
            'data': ['data', 'Data', 'DATE'],
            'descricao': ['lançamento', 'lancamento', 'LANCAMENTO'],
            'valor': ['valor (R$)', 'valor', 'VALOR']
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
            
            # Initialize progress
            df = pd.read_excel(filepath)
            
            # Find data rows (skip header)
            start_row = df[df['data'].astype(str).str.contains('/|data', case=False, na=False)].index[0]
            df = df.iloc[start_row:].reset_index(drop=True)
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            
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
                    # Skip balance rows
                    if 'SALDO' in str(row[desc_col]).upper():
                        continue

                    # Update progress
                    upload_progress[process_id].update({
                        'current': index + 1,
                        'message': f'Processando linha {index + 1} de {total_rows}'
                    })

                    # Process data
                    date = pd.to_datetime(row[data_col], format='%d/%m/%Y').date()
                    description = str(row[desc_col]).strip()
                    
                    # Handle value
                    value_str = str(row[valor_col])
                    if pd.isna(value_str) or value_str == '':
                        continue
                        
                    value = float(value_str.replace('.', '').replace(',', '.'))

                    # Determine transaction type
                    transaction_type = self.determine_transaction_type(description, value)

                    # Insert transaction
                    cursor.execute('''
                        INSERT INTO transactions (date, description, value, type, transaction_type)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        date.strftime('%Y-%m-%d'),
                        description,
                        value,
                        transaction_type,
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

    def determine_transaction_type(self, description, value):
        description = description.upper()
        if 'PIX' in description:
            return 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
        elif 'TED' in description:
            return 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
        elif 'SISPAG' in description:
            return 'PAGAMENTO'
        elif 'TAR' in description or 'TAXA' in description:
            return 'TARIFA'
        elif 'CH COMPENSADO' in description:
            return 'CHEQUE'
        return 'OUTROS'