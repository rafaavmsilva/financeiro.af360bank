from .base import BankReader
import pandas as pd
import os

class ItauReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Itaú"
    
    def get_bank_name(self):
        return self.name

    def find_data_start(self, df):
        for idx, row in df.iterrows():
            if any(str(val).strip().lower() == 'data' for val in row if pd.notna(val)):
                return idx
        return None

    def process_file(self, filepath, process_id, upload_progress):
        try:
            df = pd.read_excel(filepath)
            data_start = self.find_data_start(df)
            
            if data_start is None:
                raise ValueError("Header não encontrado")
            
            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['data', 'lancamento', 'ag_origem', 'valor', 'saldo']
            df = df[df['valor'].notna()]
            
            total_rows = len(df)
            processed_rows = 0
            conn = self.get_db_connection()
            cursor = conn.cursor()

            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]

                for _, row in batch.iterrows():
                    try:
                        date = self.parse_date(row['data'])
                        if not date:
                            continue
                            
                        description = str(row['lancamento']).strip()
                        value = float(str(row['valor']).replace('.', '').replace(',', '.'))
                        
                        cursor.execute('''
                            INSERT INTO transactions 
                            (date, description, value, type, transaction_type)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            date.strftime('%Y-%m-%d'),
                            description,
                            value,
                            'receita' if value > 0 else 'despesa',
                            self.determine_transaction_type(description, value)
                        ))
                        processed_rows += 1

                    except Exception as e:
                        print(f"Erro na linha: {str(e)}")
                        continue

                conn.commit()
                upload_progress[process_id].update({
                    'current': start_idx + len(batch),
                    'total': total_rows,
                    'message': f'Processando... {processed_rows}/{total_rows}'
                })

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

    def determine_transaction_type(self, description, value):
        description = description.upper()
        if 'PIX' in description:
            return 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
        elif 'TED' in description:
            return 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
        return 'OUTROS'