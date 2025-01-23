from .base import BankReader
import pandas as pd
from datetime import datetime
import os

class SantanderReader(BankReader):
    def process_file(self, filepath, process_id, upload_progress):
        try:
            # Read header first
            header_df = pd.read_excel(filepath, nrows=20)
            data_start = self.find_data_start(header_df)
            del header_df

            if data_start is None:
                raise ValueError("Header não encontrado")

            # Read actual data
            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            df = df.drop(['', 'Saldo'], axis=1)
            df = df[df['Data'].notna()]
            df = df[~df['Histórico'].str.contains('SALDO', case=False)]

            total_rows = len(df)
            processed_rows = 0
            conn = self.get_db_connection()
            cursor = conn.cursor()

            try:
                for start_idx in range(0, total_rows, self.batch_size):
                    end_idx = min(start_idx + self.batch_size, total_rows)
                    batch = df.iloc[start_idx:end_idx]

                    upload_progress[process_id].update({
                        'current': start_idx,
                        'total': total_rows,
                        'message': f'Processando {start_idx + 1} de {total_rows}'
                    })

                    for _, row in batch.iterrows():
                        try:
                            date = self.parse_date(row['Data'])
                            if not date:
                                continue

                            value = float(str(row['Valor']).replace('R$', '').strip().replace('.', '').replace(',', '.'))
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

                        except Exception as e:
                            print(f"Erro na linha: {e}")
                            continue

                    conn.commit()

                upload_progress[process_id].update({
                    'status': 'completed',
                    'current': total_rows,
                    'message': f'Concluído: {processed_rows} transações'
                })

            finally:
                conn.close()
                os.remove(filepath)

            return True

        except Exception as e:
            if 'conn' in locals():
                conn.close()
            raise