from .base import BankReader
import pandas as pd
import re
import os
from datetime import datetime

class ItauReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Itaú"
        self.column_mapping = {
            'data': ['data', 'Data', 'DATA'],
            'descricao': ['lançamento', 'lancamento', 'LANCAMENTO'],
            'valor': ['valor (R$)', 'valor', 'VALOR']
        }

    def get_bank_name(self):
        return self.name

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
        elif 'MOV TIT' in description:
            return 'RECEITA' if value > 0 else 'DESPESA'
        return 'OUTROS'

    def process_file(self, filepath, process_id, upload_progress):
        try:
            # Read Excel with all rows
            df = pd.read_excel(filepath)
            
            # Find data start (where column headers begin)
            data_start = None
            for idx, row in df.iterrows():
                if 'data' in str(row[0]).lower():
                    data_start = idx
                    break
                    
            if data_start is None:
                raise ValueError("Não foi possível encontrar o início dos dados")
                
            # Read data with correct header
            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['data', 'lancamento', 'ag_origem', 'valor', 'saldo']
            
            # Remove balance rows and empty rows
            df = df[
                ~df['lancamento'].astype(str).str.contains('SALDO', case=False, na=False) & 
                df['valor'].notna()
            ]
            
            total_rows = len(df)
            processed_rows = 0
            
            # Initialize progress
            upload_progress[process_id].update({
                'status': 'processing',
                'current': 0,
                'total': total_rows,
                'message': 'Iniciando processamento...'
            })

            conn = self.get_db_connection()
            cursor = conn.cursor()

            for index, row in df.iterrows():
                try:
                    # Update progress
                    upload_progress[process_id].update({
                        'current': index + 1,
                        'message': f'Processando linha {index + 1} de {total_rows}'
                    })

                    # Process date
                    date = pd.to_datetime(row['data']).date()
                    
                    # Process description
                    description = str(row['lancamento']).strip()
                    
                    # Process value
                    value = float(str(row['valor']).replace('.', '').replace(',', '.'))
                    
                    # Determine transaction type
                    transaction_type = self.determine_transaction_type(description, value)

                    # Insert transaction
                    cursor.execute('''
                        INSERT INTO transactions 
                        (date, description, value, type, transaction_type)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        date.strftime('%Y-%m-%d'),
                        description,
                        value,
                        'receita' if value > 0 else 'despesa',
                        transaction_type
                    ))

                    processed_rows += 1

                except Exception as e:
                    print(f"Erro ao processar linha {index + 1}: {str(e)}")
                    continue

            conn.commit()
            conn.close()
            
            # Update final status
            upload_progress[process_id].update({
                'status': 'completed',
                'current': total_rows,
                'message': f'Processamento concluído! {processed_rows} transações importadas.'
            })

            # Remove temporary file
            os.remove(filepath)
            
            return True

        except Exception as e:
            upload_progress[process_id].update({
                'status': 'error',
                'message': f'Erro: {str(e)}'
            })
            if 'conn' in locals():
                conn.close()
            raise