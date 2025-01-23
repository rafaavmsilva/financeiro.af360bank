import os
import pandas as pd
from .base import BankReader
import gc

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.batch_size = 20
        self.max_retries = 3
        
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
            # Initialize progress
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': 0,
                'message': 'Iniciando...'
            }
            
            # Count rows first
            with pd.read_excel(filepath) as reader:
                total_rows = len(reader)
            
            # Process in batches
            df = pd.read_excel(filepath)
            data_start = self.find_data_start(df)
            
            if data_start is None:
                raise ValueError("Header não encontrado")
            
            df = df.iloc[data_start:]
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            df = df.drop(['', 'Saldo'], axis=1)
            df = df[df['Data'].notna()]

            total_batches = math.ceil(len(df) / self.batch_size)
            processed_rows = 0
            conn = self.get_db_connection()
            cursor = conn.cursor()

            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min((batch_num + 1) * self.batch_size, len(df))
                batch = df.iloc[start_idx:end_idx].copy()

                for _, row in batch.iterrows():
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            date = pd.to_datetime(row['Data'], format='%d/%m/%Y').date()
                            value = float(str(row['Valor']).replace('R$', '').replace('.', '').replace(',', '.'))
                            description = str(row['Histórico']).strip()
                            document = str(row['Documento']).strip()
                            
                            cursor.execute('''
                                INSERT INTO transactions 
                                (date, description, value, type, transaction_type, document)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                date.strftime('%Y-%m-%d'),
                                description,
                                value,
                                'receita' if value > 0 else 'despesa',
                                self.determine_transaction_type(description, value),
                                document if document != 'nan' else None
                            ))
                            processed_rows += 1
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == self.max_retries:
                                print(f"Failed after {self.max_retries} retries: {str(e)}")

                conn.commit()
                del batch
                gc.collect()
                
                upload_progress[process_id].update({
                    'current': processed_rows,
                    'total': total_rows,
                    'message': f'Processando... {processed_rows}/{total_rows}'
                })

            os.remove(filepath)
            return True

        except Exception as e:
            if conn:
                conn.close()
            raise e
        finally:
            if conn:
                conn.close()