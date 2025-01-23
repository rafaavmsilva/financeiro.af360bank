from .base import BankReader
import pandas as pd
from datetime import datetime
import os

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.column_mapping = {
            'data': ['data', 'Data', 'DATA'],
            'descricao': ['Histórico', 'histórico', 'HISTORICO'],
            'documento': ['Documento', 'documento', 'DOCUMENTO'],
            'valor': ['Valor', 'valor', 'VALOR']
        }

    def get_bank_name(self):
        return self.name

    def find_data_start(self, df):
        for idx, row in df.iterrows():
            if 'Data' in str(row[0]):
                return idx
        return None

    def process_file(self, filepath, process_id, upload_progress):
        try:
            # Read Excel with all rows
            df = pd.read_excel(filepath)
            
            # Find data start
            data_start = self.find_data_start(df)
            if data_start is None:
                raise ValueError("Não foi possível encontrar o início dos dados")
                
            # Read data with correct header
            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            
            # Clean data
            df = df.drop(['', 'Saldo'], axis=1)
            df = df[~df['Histórico'].astype(str).str.contains('SALDO', case=False, na=False)]
            df = df[df['Valor'].notna()]
            
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

                    # Process date with explicit format
                    date_str = str(row['Data']).strip()
                    date = datetime.strptime(date_str, '%d/%m/%Y').date()
                    
                    # Process description
                    description = str(row['Histórico']).strip()
                    
                    # Process value
                    value_str = str(row['Valor']).replace('R$', '').strip()
                    value = float(value_str.replace('.', '').replace(',', '.'))
                    
                    # Get document
                    document = str(row['Documento']).strip()
                    
                    # Determine transaction type
                    transaction_type = self.determine_transaction_type(description, value)

                    # Insert transaction
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
            if 'conn' in locals():
                conn.close()
            raise

    def determine_transaction_type(self, description, value):
        description = description.upper()
        if 'PIX' in description:
            return 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
        elif 'TED' in description:
            return 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
        elif 'PAGAMENTO' in description:
            return 'PAGAMENTO'
        elif 'TARIFA' in description:
            return 'TARIFA'
        elif 'IOF' in description:
            return 'IOF'
        elif 'RESGATE' in description:
            return 'RESGATE'
        return 'OUTROS'