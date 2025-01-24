import pandas as pd
import os
import time
from functools import wraps
from datetime import datetime

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def retry_on_error(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:  # Last attempt
                        raise Exception(f"Failed after {max_retries} attempts: {str(e)}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def find_matching_column(df, possible_names):
    """Find matching column with smarter detection"""
    for name in possible_names:
        # Direct match
        if name in df.columns:
            return name
        # Case insensitive match    
        matches = [col for col in df.columns if str(name).lower() in str(col).lower()]
        if matches:
            return matches[0]
    return None

def extract_transaction_info(historico, valor):
    """Extract detailed transaction information from the historic text"""
    historico = historico.upper()
    info = {
        'tipo': None,
        'valor': valor,
        'identificador': None,
        'document': None,
        'description': historico  # Mantém a descrição original por padrão
    }
    
    # Mapeamento de palavras-chave para tipos de transação
    tipo_mapping = {
        'PIX RECEBIDO': ['PIX RECEBIDO'],
        'PIX ENVIADO': ['PIX ENVIADO'],
        'TED RECEBIDA': ['TED RECEBIDA', 'TED CREDIT'],
        'TED ENVIADA': ['TED ENVIADA', 'TED DEBIT'],
        'PAGAMENTO': ['PAGAMENTO', 'PGTO', 'PAG'],
        'TARIFA': ['TARIFA', 'TAR'],
        'IOF': ['IOF'],
        'RESGATE': ['RESGATE'],
        'APLICACAO': ['APLICACAO', 'APLICAÇÃO'],
        'COMPRA': ['COMPRA'],
        'COMPENSACAO': ['COMPENSACAO', 'COMPENSAÇÃO'],
        'CHEQUE DEVOLVIDO': ['CHEQUE DEVOLVIDO', 'CH DEVOLVIDO'],
        'JUROS': ['JUROS'],
        'MULTA': ['MULTA'],
        'ANTECIPACAO': ['ANTECIPACAO', 'ANTECIPAÇÃO'],
        'CHEQUE EMITIDO': ['CHEQUE EMITIDO', 'CH EMITIDO']
    }
    
    # Procura por padrões específicos
    for tipo, keywords in tipo_mapping.items():
        if any(keyword in historico for keyword in keywords):
            info['tipo'] = tipo
            break
    
    # Se nenhum tipo específico foi encontrado
    if info['tipo'] is None:
        info['tipo'] = 'OUTROS'
    
    # Procura por CNPJ no histórico
    if info['tipo'] in ['PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO']:
        import re
        
        # Procura por CNPJ em formato texto (ex: "CNPJ 12345678901234")
        cnpj_text_match = re.search(r'CNPJ[:\s]+(\d{12,14})', historico)
        if cnpj_text_match:
            cnpj = cnpj_text_match.group(1)
            # Remove zeros à esquerda e garante que tenha 14 dígitos
            cnpj = str(int(cnpj)).zfill(14)
            info['document'] = cnpj
            
            # Mantém a descrição original para processamento posterior
            if info['tipo'] == 'PAGAMENTO':
                info['description'] = historico.replace(cnpj_text_match.group(0), f"CNPJ {str(int(cnpj))}")
        else:
            # Procura por sequência de 14 dígitos
            cnpj_match = re.search(r'\b\d{14}\b', historico)
            if cnpj_match:
                cnpj = cnpj_match.group()
                # Remove zeros à esquerda
                cnpj = str(int(cnpj)).zfill(14)
                info['document'] = cnpj
            else:
                # Procura por CNPJ formatado
                cnpj_match = re.search(r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b', historico)
                if cnpj_match:
                    cnpj = ''.join(filter(str.isdigit, cnpj_match.group()))
                    # Remove zeros à esquerda
                    cnpj = str(int(cnpj)).zfill(14)
                    info['document'] = cnpj
    
    # Tenta extrair identificador após o tipo de transação
    if info['tipo'] in ['PIX RECEBIDO', 'PIX ENVIADO', 'TED RECEBIDA', 'TED ENVIADA']:
        parts = historico.split(info['tipo'])
        if len(parts) > 1:
            # Remove espaços e caracteres especiais, mantém apenas números
            identificador = ''.join(filter(str.isdigit, parts[1].strip()))
            if identificador and identificador != info.get('document', ''):  # Não use o CNPJ como identificador
                info['identificador'] = identificador
    
    return info

def get_transaction_type(historico):
    """Determine transaction type based on Histórico"""
    historico = historico.upper()
    if any(keyword in historico for keyword in ['PIX RECEBIDO', 'TED RECEBIDA', 'RECEBIDO']):
        return 'receita'
    elif any(keyword in historico for keyword in ['PIX ENVIADO', 'TED ENVIADA', 'ENVIADO']):
        return 'despesa'
    return 'outros'

def find_header_row(df):
    """Find header row after agency info"""
    for idx, row in df.iterrows():
        # Check if row contains agency info
        if '0715' in str(row.iloc[0]):
            continue
        # Find actual header row    
        if 'Data' in str(row.iloc[0]) and 'Histórico' in str(row.iloc[1]):
            return idx
    return 0

@retry_on_error()
def process_excel_file(file):
    """Process Excel file and extract transaction data"""
    try:
        df = pd.read_excel(file)
        
        # Find the header row
        header_row = find_header_row(df)
        if header_row > 0:
            # Get the header row values
            new_columns = [str(val).strip() if not pd.isna(val) else f'Column_{i}' 
                         for i, val in enumerate(df.iloc[header_row])]
            df.columns = new_columns
            df = df.iloc[header_row + 1:].reset_index(drop=True)
        
        # Find relevant columns
        data_col = find_matching_column(df, ['Data', 'DATE', 'DT'])
        historico_col = find_matching_column(df, ['Histórico', 'HISTORIC', 'DESCRIÇÃO', 'DESCRICAO'])
        valor_col = find_matching_column(df, ['Valor', 'VALUE', 'QUANTIA'])
        
        if not all([data_col, historico_col, valor_col]):
            raise Exception("Não foi possível encontrar todas as colunas necessárias")
        
        transactions = []
        
        for _, row in df.iterrows():
            try:
                # Get date
                data = row[data_col]
                if pd.isna(data):
                    continue
                    
                # Handle different date formats
                try:
                    if isinstance(data, str):
                        # Try different date formats
                        try:
                            data = datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d')
                        except ValueError:
                            try:
                                data = datetime.strptime(data, '%Y-%m-%d').strftime('%Y-%m-%d')
                            except ValueError:
                                print(f"Erro ao processar linha {_}: 'Data'")
                                print(f"Dados da linha: {dict(row)}")
                                continue
                    elif isinstance(data, datetime):
                        data = data.strftime('%Y-%m-%d')
                    else:
                        try:
                            data = pd.to_datetime(data).strftime('%Y-%m-%d')
                        except:
                            print(f"Erro ao processar linha {_}: 'Data'")
                            print(f"Dados da linha: {dict(row)}")
                            continue
                except Exception as e:
                    print(f"Erro ao processar linha {_}: 'Data'")
                    print(f"Dados da linha: {dict(row)}")
                    continue
                
                # Get description
                historico = str(row[historico_col]).strip()
                if pd.isna(historico) or not historico:
                    continue
                
                # Get value and convert to float
                valor = row[valor_col]
                if pd.isna(valor):
                    continue
                    
                if isinstance(valor, (int, float)):
                    valor = float(valor)
                else:
                    valor_str = str(valor).replace('R$', '').strip()
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                
                # Extract transaction info
                info = extract_transaction_info(historico, valor)
                
                transactions.append({
                    'date': data,
                    'description': info['description'],
                    'value': valor,
                    'type': info['tipo'],
                    'document': info.get('document', ''),
                    'identifier': info.get('identificador', ''),
                    'transaction_type': 'receita' if valor > 0 else 'despesa'
                })
                
            except Exception as e:
                print(f"Erro ao processar linha: {e}")
                continue
        
        if not transactions:
            raise Exception("Nenhuma transação válida encontrada no arquivo")
            
        return transactions
        
    except Exception as e:
        raise Exception(f"Erro ao processar arquivo Excel: {str(e)}")

def main():
    try:
        # Get the Excel file path from user
        print("Please place your Excel file in the same directory as this script.")
        file_name = input("Enter the Excel file name (e.g., 'data.xlsx'): ")
        
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, file_name)
        
        # Process the file
        processed_data = process_excel_file(file_path)
        print(f"Successfully processed {len(processed_data)} transactions")
        for item in processed_data:
            print(item)
        return processed_data
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    main()
