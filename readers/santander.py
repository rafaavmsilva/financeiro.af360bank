import os

class SantanderReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Santander"

    def get_bank_name(self):
        return self.name

    def find_data_start(self, df):
        for idx, row in df.iterrows():
            if 'Data' in str(row.iloc[0]):
                return idx
        return None

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
                            # Validate required fields
                            date = self.parse_date(row['Data'])
                            value = self.validate_value(row['Valor'])
                            
                            if not date or not value:
                                continue
                                
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