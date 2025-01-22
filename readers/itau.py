from .base import BankReader
import pandas as pd
import re
from datetime import datetime

class ItauReader(BankReader):
    def __init__(self):
        super().__init__()
        self.name = "Itaú"

    def process_file(self, filepath, process_id, upload_progress):
        try:
            # Read Excel file skipping header rows
            df = pd.read_excel(filepath, header=None)
            
            # Find the header row (where 'data' appears)
            header_row = df[df[0].astype(str).str.contains('data', case=False)].index[0]
            
            # Read data after header row
            df = pd.read_excel(filepath, skiprows=header_row)
            
            # Rename columns
            df.columns = ['data', 'lancamento', 'ag_origem', 'valor', 'saldo']
            
            # Remove balance rows
            df = df[~df['lancamento'].astype(str).str.contains('SALDO', case=False, na=False)]
            
            total_rows = len(df)
            processed_rows = 0
            
            # Initialize progress
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': total_rows,
                'message': 'Processando arquivo...'
            }
            
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            for index, row in df.iterrows():
                try:
                    # Update progress
                    upload_progress[process_id]['current'] = index + 1
                    upload_progress[process_id]['message'] = f'Processando linha {index + 1} de {total_rows}'
                    
                    # Skip empty or invalid rows
                    if pd.isna(row['data']) or pd.isna(row['valor']):
                        continue
                    
                    # Process date
                    date = pd.to_datetime(row['data']).date()
                    
                    # Process description
                    description = str(row['lancamento']).strip()
                    
                    # Process value (already in correct format for Itaú)
                    value = float(row['valor'])
                    
                    # Determine transaction type
                    transaction_type = self.determine_transaction_type(description, value)
                    
                    # Insert into database
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
            
            conn.commit()
            conn.close()
            
            # Update final status
            upload_progress[process_id].update({
                'status': 'completed',
                'current': total_rows,
                'message': f'Processamento concluído! {processed_rows} transações importadas.'
            })
            
            return True
            
        except Exception as e:
            print(f"Erro no processamento: {str(e)}")
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
        elif 'MOV TIT' in description:
            return 'RECEITA' if value > 0 else 'DESPESA'
        return 'OUTROS'