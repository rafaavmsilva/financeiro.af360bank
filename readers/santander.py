import os
import pandas as pd
from .base import BankReader
import gc

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"
        self.batch_size = 10
        
    def get_bank_name(self):
        return self.name
        
    def find_data_start(self, df):
        # Find actual data start after headers
        for idx, row in df.iterrows():
            if any(str(val).strip() == 'Data' for val in row if pd.notna(val)):
                return idx + 1  # Skip header row
        return None

    def process_file(self, filepath, process_id, upload_progress):
        try:
            # Read first chunk to find header
            header_df = pd.read_excel(filepath, nrows=20)
            data_start = self.find_data_start(header_df)
            del header_df
            gc.collect()

            if data_start is None:
                raise ValueError("Header não encontrado")

            # Read actual data
            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            df = df.drop(['', 'Saldo'], axis=1)
            df = df[df['Data'].notna()]
            df = df[~df['Histórico'].astype(str).str.contains('SALDO', case=False)]

            total_rows = len(df)
            processed_rows = 0
            conn = self.get_db_connection()
            cursor = conn.cursor()

            try:
                for start_idx in range(0, total_rows, self.batch_size):
                    batch = df.iloc[start_idx:min(start_idx + self.batch_size, total_rows)]
                    
                    for _, row in batch.iterrows():
                        try:
                            # Validate date
                            if pd.isna(row['Data']):
                                continue
                                
                            date = pd.to_datetime(row['Data'], format='%d/%m/%Y').date()
                            
                            # Validate value
                            value_str = str(row['Valor']).replace('R$', '').strip()
                            if not value_str or pd.isna(value_str):
                                continue
                                
                            value = float(value_str.replace('.', '').replace(',', '.'))
                            
                            # Get description and document
                            description = str(row['Histórico']).strip()
                            document = str(row['Documento']).strip()
                            
                            if not description:
                                continue
                                
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

                        except Exception as e:
                            print(f"Erro ao processar linha: {str(e)}")
                            continue

                    conn.commit()
                    gc.collect()
                    
                    upload_progress[process_id].update({
                        'current': start_idx + len(batch),
                        'total': total_rows,
                        'message': f'Processando... {processed_rows}/{total_rows}'
                    })

            finally:
                conn.close()
                os.remove(filepath)

            upload_progress[process_id].update({
                'status': 'completed',
                'message': f'Concluído: {processed_rows} transações'
            })

            return True

        except Exception as e:
            if 'conn' in locals():
                conn.close()
            raise