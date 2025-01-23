import os
import pandas as pd
from readers.base import BankReader

class SantanderReader(BankReader):
    def process_file(self, filepath, process_id, upload_progress):
        try:
            df = pd.read_excel(filepath, nrows=20)
            data_start = self.find_data_start(df)
            del df

            if data_start is None:
                raise ValueError("Header não encontrado")

            df = pd.read_excel(filepath, skiprows=data_start)
            df.columns = ['Data', '', 'Histórico', 'Documento', 'Valor', 'Saldo']
            df = df.drop(['', 'Saldo'], axis=1)
            df = df[df['Data'].notna()]

            total_rows = len(df)
            processed_rows = 0
            conn = self.get_db_connection()
            cursor = conn.cursor()

            try:
                for start_idx in range(0, total_rows, self.batch_size):
                    end_idx = min(start_idx + self.batch_size, total_rows)
                    batch = df.iloc[start_idx:end_idx]

                    for _, row in batch.iterrows():
                        try:
                            date = self.parse_date(row['Data'])
                            value = self.validate_value(row['Valor'])
                            
                            if not date or value is None:
                                print(f"Invalid date or value: date={row['Data']}, value={row['Valor']}")
                                continue
                            
                            description = str(row['Histórico']).strip()
                            if not description:
                                continue

                            document = str(row['Documento']).strip()
                            transaction_type = self.determine_transaction_type(description, value)
                            
                            if not transaction_type:
                                print(f"Missing transaction type for: {description}")
                                continue

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