import os
import pandas as pd
from .base import BankReader
import gc

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.batch_size = 5
        
    def get_bank_name(self):
        return self.name
        
    def find_data_start(self, df):
        for idx, row in df.iterrows():
            if any(str(val).strip() == 'Data' for val in row if pd.notna(val)):
                return idx + 1
        return None

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

    def process_file(self, filepath, process_id, upload_progress):
        conn = None
        try:
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': 0,
                'message': 'Iniciando...'
            }

            # Count total rows first
            total_df = pd.read_excel(filepath)
            total_rows = len(total_df)
            del total_df
            gc.collect()

            # Process in chunks
            reader = pd.read_excel(filepath, chunksize=self.chunk_size)
            processed_rows = 0
            data_start_found = False
            conn = self.get_db_connection()
            cursor = conn.cursor()

            for chunk_idx, chunk in enumerate(reader):
                if not data_start_found:
                    data_start = self.find_data_start(chunk)
                    if data_start is None:
                        continue
                    chunk = chunk.iloc[data_start:]
                    data_start_found = True

                chunk.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
                chunk = chunk.drop(['', 'Saldo'], axis=1)
                chunk = chunk[chunk['Data'].notna()]

                for idx, row in chunk.iterrows():
                    try:
                        date = pd.to_datetime(row['Data'], format='%d/%m/%Y').date()
                        value = float(str(row['Valor']).replace('R$', '').replace('.', '').replace(',', '.'))
                        description = str(row['Histórico']).strip()
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

                        if processed_rows % self.commit_interval == 0:
                            conn.commit()
                            gc.collect()

                    except Exception as e:
                        print(f"Erro ao processar linha {processed_rows}: {str(e)}")
                        continue

                upload_progress[process_id].update({
                    'current': processed_rows,
                    'total': total_rows,
                    'message': f'Processando... {processed_rows}/{total_rows}'
                })

            conn.commit()
            os.remove(filepath)
            return True

        except Exception as e:
            raise e
        finally:
            if conn:
                conn.close()