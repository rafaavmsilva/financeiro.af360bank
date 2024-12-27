from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, session
from datetime import datetime, timedelta
from itsdangerous import URLSafeTimedSerializer, SignatureExpired
import sqlite3
import os
import pandas as pd
from werkzeug.utils import secure_filename
from read_excel import process_excel_file
from functools import wraps
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import uuid
import threading
from auth_client import AuthClient

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=1)  # Set session lifetime to 1 hour

# Global variables
upload_progress = {}  # Dictionary to track file upload progress
cnpj_cache = {}  # Cache global para armazenar informações de CNPJs
failed_cnpjs = set()  # Conjunto para armazenar CNPJs que falharam

# Initialize AuthClient
auth_client = AuthClient(
    auth_server_url=os.getenv('AUTH_SERVER_URL', 'https://af360bank.onrender.com'),
    app_name=os.getenv('APP_NAME', 'financeiro')
)
auth_client.init_app(app)

# Ensure the upload and instance folders exist
for folder in ['instance', 'uploads']:
    if not os.path.exists(folder):
        os.makedirs(folder)

# Rate limiting configuration
RATE_LIMIT_WINDOW = 60  # seconds
REQUEST_LIMIT = 60      # requests per window
request_history = {}

@app.route('/auth')
def auth():
    token = request.args.get('token')
    if not token:
        return redirect('https://af360bank.onrender.com/login')
    
    verification = auth_client.verify_token(token)
    if not verification or not verification.get('valid'):
        return redirect('https://af360bank.onrender.com/login')
    
    # Set session variables
    session['token'] = token
    session['authenticated'] = True
    session.permanent = True  # Make the session last longer
    
    return redirect(url_for('index'))

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = session.get('token')
        if not token:
            return redirect('https://af360bank.onrender.com/login')
        
        verification = auth_client.verify_token(token)
        if not verification or not verification.get('valid'):
            session.clear()
            return redirect('https://af360bank.onrender.com/login')
        
        return f(*args, **kwargs)
    return decorated_function
    
def rate_limit():
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            now = time.time()
            client_ip = request.remote_addr
            
            # Initialize or clean old requests
            if client_ip not in request_history:
                request_history[client_ip] = []
            request_history[client_ip] = [t for t in request_history[client_ip] if t > now - RATE_LIMIT_WINDOW]
            
            # Check rate limit
            if len(request_history[client_ip]) >= REQUEST_LIMIT:
                return jsonify({'error': 'Rate limit exceeded. Please try again later.'}), 429
            
            # Add current request
            request_history[client_ip].append(now)
            return f(*args, **kwargs)
        return wrapped
    return decorator

# Database connection helper
def get_db_connection():
    # Ensure instance directory exists
    os.makedirs('instance', exist_ok=True)
    conn = sqlite3.connect('instance/financas.db')
    conn.row_factory = sqlite3.Row
    return conn

# Database initialization
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            description TEXT NOT NULL,
            value REAL NOT NULL,
            type TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            document TEXT
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_document ON transactions(document)')
    
    conn.commit()
    conn.close()

# Initialize the database when the app starts
init_db()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'xls', 'xlsx'}

def get_company_info(cnpj):
    """Busca informações da empresa, usando cache se disponível"""
    # Verifica se já está no cache
    if cnpj in cnpj_cache:
        return cnpj_cache[cnpj]
    
    try:
        response = requests.get(f'https://brasilapi.com.br/api/cnpj/v1/{cnpj}')
        if response.status_code == 200:
            company_info = response.json()
            # Armazena no cache
            cnpj_cache[cnpj] = company_info
            if cnpj in failed_cnpjs:
                failed_cnpjs.remove(cnpj)
            return company_info
        else:
            failed_cnpjs.add(cnpj)
    except Exception as e:
        print(f"Erro ao buscar informações da empresa: {e}")
        failed_cnpjs.add(cnpj)
    return None

@app.route('/')
@login_required
def index():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    return render_template('index.html', active_page='index')

@app.route('/upload', methods=['POST'])
@login_required
@rate_limit()
def upload_file():
    try:
        if not session.get('authenticated'):
            return redirect('https://af360bank.onrender.com/login')
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'Nenhum arquivo selecionado'
            })
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'Nenhum arquivo selecionado'
            })
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # Salva o arquivo
            file.save(filepath)
            
            # Inicializa o progresso
            process_id = str(uuid.uuid4())
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': 0,
                'message': 'Iniciando processamento...'
            }
            
            # Processa o arquivo em uma thread separada
            thread = threading.Thread(target=process_file_with_progress, args=(filepath, process_id))
            thread.start()
            
            return jsonify({
                'success': True,
                'process_id': process_id,
                'message': 'Arquivo enviado e sendo processado'
            })
        
        return jsonify({
            'success': False,
            'message': 'Tipo de arquivo não permitido'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Erro ao processar arquivo: {str(e)}'
        })

def find_matching_column(df, column_names):
    for col in df.columns:
        if col.upper() in [name.upper() for name in column_names]:
            return col
    return None

def extract_transaction_info(description, value):
    transaction_info = {
        'description': description,
        'tipo': None,
        'document': None
    }
    
    # Detecta o tipo de transação e CNPJ
    if 'PIX' in description.upper():
        transaction_info['tipo'] = 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
    elif 'TED' in description.upper():
        transaction_info['tipo'] = 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
    elif 'PAGAMENTO' in description.upper():
        transaction_info['tipo'] = 'PAGAMENTO'
    
    # Extrai CNPJ se presente
    if transaction_info['tipo']:
        enriched_description = extract_and_enrich_cnpj(description, transaction_info['tipo'])
        transaction_info['description'] = enriched_description
    
    return transaction_info

def process_file_with_progress(filepath, process_id):
    try:
        print(f"Iniciando processamento do arquivo: {filepath}")
        
        # Lê o arquivo Excel
        df = pd.read_excel(filepath)
        print(f"Arquivo lido com sucesso. Total de linhas: {len(df)}")
        print(f"Colunas encontradas: {df.columns.tolist()}")
        
        total_rows = len(df)
        upload_progress[process_id]['total'] = total_rows
        upload_progress[process_id]['message'] = 'Lendo arquivo...'
        
        # Encontra as colunas corretas
        data_col = find_matching_column(df, ['Data', 'DATE', 'DT', 'AGENCIA'])
        desc_col = find_matching_column(df, ['Histórico', 'HISTORIC', 'DESCRIÇÃO', 'DESCRICAO', 'CONTA'])
        valor_col = find_matching_column(df, ['Valor', 'VALUE', 'QUANTIA', 'Unnamed: 4'])
        
        if not all([data_col, desc_col, valor_col]):
            raise Exception(f"Colunas necessárias não encontradas. Colunas disponíveis: {df.columns.tolist()}")
        
        # Conecta ao banco de dados
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Processa cada linha
        processed_rows = 0
        for index, row in df.iterrows():
            # Atualiza o progresso
            upload_progress[process_id]['current'] = index + 1
            upload_progress[process_id]['message'] = f'Processando linha {index + 1} de {total_rows}'
            
            try:
                # Processa a data
                data = row[data_col]
                if pd.isna(data):
                    continue
                
                try:
                    if isinstance(data, str):
                        try:
                            date = datetime.strptime(data, '%d/%m/%Y').date()
                        except ValueError:
                            try:
                                date = datetime.strptime(data, '%Y-%m-%d').date()
                            except ValueError:
                                print(f"Erro ao processar linha {index + 1}: 'Data'")
                                print(f"Dados da linha: {row.to_dict()}")
                                continue
                    elif isinstance(data, datetime):
                        date = data.date()
                    else:
                        try:
                            date = pd.to_datetime(data).date()
                        except:
                            print(f"Erro ao processar linha {index + 1}: 'Data'")
                            print(f"Dados da linha: {row.to_dict()}")
                            continue
                except Exception as e:
                    print(f"Erro ao processar linha {index + 1}: 'Data'")
                    print(f"Dados da linha: {row.to_dict()}")
                    continue
                
                # Processa a descrição
                description = str(row[desc_col]).strip()
                if pd.isna(description) or not description:
                    continue
                
                # Processa o valor
                value = row[valor_col]
                if pd.isna(value):
                    continue
                
                if isinstance(value, (int, float)):
                    value = float(value)
                else:
                    value_str = str(value).replace('R$', '').strip()
                    value = float(value_str.replace('.', '').replace(',', '.'))
                
                print(f"Processando linha {index + 1}: Data={date}, Valor={value}")
                
                # Detecta o tipo de transação
                description_upper = description.upper()
                transaction_type = None
                
                # Mapeamento de palavras-chave para tipos de transação
                type_mapping = {
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
                    'CHEQUE': ['CHEQUE'],
                    'TRANSFERENCIA': ['TRANSFERENCIA', 'TRANSF'],
                    'JUROS': ['JUROS'],
                    'MULTA': ['MULTA']
                }
                
                # Primeiro, tenta encontrar o tipo pela descrição
                for tipo, keywords in type_mapping.items():
                    if any(keyword in description_upper for keyword in keywords):
                        transaction_type = tipo
                        break
                
                # Se não encontrou tipo específico, usa PIX/TED baseado no valor
                if transaction_type is None:
                    if 'PIX' in description_upper:
                        transaction_type = 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
                    elif 'TED' in description_upper:
                        transaction_type = 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
                    else:
                        # Tipo genérico baseado no valor
                        transaction_type = 'CREDITO' if value > 0 else 'DEBITO'
                
                print(f"Tipo de transação detectado: {transaction_type}")
                
                # Extrai CNPJ se presente
                cnpj = None
                if transaction_type:
                    # Find sequence of 14 digits that could be a CNPJ
                    import re
                    
                    # Try different CNPJ patterns
                    cnpj_patterns = [
                        r'CNPJ[:\s]*(\d{14,15})',  # CNPJ followed by 14 or 15 digits
                        r'CNPJ[:\s]*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})',  # CNPJ followed by formatted number
                        r'\b(\d{14,15})\b',  # Just 14 or 15 digits
                        r'\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b'  # Formatted CNPJ
                    ]
                    
                    cnpj_match = None
                    for pattern in cnpj_patterns:
                        match = re.search(pattern, description)
                        if match:
                            cnpj_match = match
                            break
                    
                    if cnpj_match:
                        # Extract CNPJ and handle 15-digit case
                        cnpj = ''.join(filter(str.isdigit, cnpj_match.group(1)))
                        if len(cnpj) == 15 and cnpj.startswith('0'):
                            cnpj = cnpj[1:]  # Remove first zero only if 15 digits
                        elif len(cnpj) != 14:
                            cnpj = None  # Invalid CNPJ length
                
                # Insere no banco de dados
                cursor.execute('''
                    INSERT INTO transactions (date, description, value, type, transaction_type, document)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (date.strftime('%Y-%m-%d'), description, value, transaction_type, 'receita' if value > 0 else 'despesa', cnpj))
                
                processed_rows += 1
                
            except Exception as row_error:
                print(f"Erro ao processar linha {index + 1}: {str(row_error)}")
                print(f"Dados da linha: {row.to_dict()}")
                continue
        
        # Commit e fecha conexão
        conn.commit()
        conn.close()
        
        print(f"Processamento concluído. Total de linhas processadas: {processed_rows}")
        
        # Atualiza status final
        upload_progress[process_id]['status'] = 'completed'
        upload_progress[process_id]['message'] = f'Processamento concluído! {processed_rows} transações importadas.'
        
        # Remove o arquivo após processamento
        os.remove(filepath)
        
    except Exception as e:
        print(f"Erro geral no processamento: {str(e)}")
        if 'df' in locals():
            print("Exemplo das primeiras linhas do DataFrame:")
            print(df.head())
        
        upload_progress[process_id]['status'] = 'error'
        upload_progress[process_id]['message'] = f'Erro: {str(e)}'

@app.route('/upload_progress/<process_id>')
@login_required
def get_upload_progress(process_id):
    """Retorna o progresso atual do upload"""
    if process_id not in upload_progress:
        return jsonify({'error': 'Process ID not found'}), 404
    
    progress_data = upload_progress[process_id]
    
    # Se o processamento foi concluído ou teve erro, remove do dicionário após alguns segundos
    if progress_data['status'] in ['completed', 'error']:
        def cleanup():
            time.sleep(30)  # Mantém o resultado por 30 segundos
            upload_progress.pop(process_id, None)
        threading.Thread(target=cleanup).start()
    
    return jsonify(progress_data)

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'time': datetime.now().isoformat(),
        'auth_server': os.getenv('AUTH_SERVER_URL'),
        'app_name': os.getenv('APP_NAME')
    })

@app.route('/recebidos')
@login_required
def recebidos():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get filters from query string
    tipo_filtro = request.args.get('tipo', 'todos')
    cnpj_filtro = request.args.get('cnpj', 'todos')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    # Get unique CNPJs for received transactions
    cursor.execute('''
        SELECT DISTINCT document
        FROM transactions 
        WHERE document IS NOT NULL 
        AND type IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO')
        AND (type != 'PAGAMENTO' OR description LIKE '%PAGAMENTO A%')
    ''')
    
    cnpjs = []
    for row in cursor.fetchall():
        if row[0]:
            company_info = get_company_info(row[0])
            if company_info:
                company_name = company_info.get('nome_fantasia') or company_info.get('razao_social', '')
                if company_name:
                    cnpjs.append({
                        'cnpj': row[0],
                        'name': f"{company_name} ({row[0]})"
                    })
    
    # Base query for received transactions
    query = '''
        SELECT date, description, value, type, document
        FROM transactions
        WHERE type IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO')
        AND (type != 'PAGAMENTO' OR description LIKE '%PAGAMENTO A%')
    '''
    
    # Add filters if necessary
    params = []
    if tipo_filtro != 'todos':
        query += " AND type = ?"
        params.append(tipo_filtro)
    
    if cnpj_filtro != 'todos':
        query += " AND document = ?"
        params.append(cnpj_filtro)
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date DESC"
    cursor.execute(query, params)
    
    transactions = []
    totals = {
        'pix_recebido': 0.0,
        'ted_recebida': 0.0,
        'pagamento': 0.0
    }
    
    for row in cursor.fetchall():
        transaction = {
            'date': row[0],
            'description': row[1],
            'value': float(row[2]),
            'type': row[3],
            'document': row[4],
            'has_company_info': False
        }
        
        # Add to corresponding total
        if transaction['type'] == 'PIX RECEBIDO':
            totals['pix_recebido'] += transaction['value']
        elif transaction['type'] == 'TED RECEBIDA':
            totals['ted_recebida'] += transaction['value']
        elif transaction['type'] == 'PAGAMENTO':
            totals['pagamento'] += abs(transaction['value'])
        
        # Get company name if CNPJ exists
        if transaction['document']:
            company_info = get_company_info(transaction['document'])
            if company_info:
                company_name = company_info.get('nome_fantasia') or company_info.get('razao_social', '')
                if company_name:
                    cnpj_sem_zeros = str(int(transaction['document']))
                    
                    if transaction['type'] == 'PAGAMENTO' and 'PAGAMENTO A' in transaction['description']:
                        transaction['description'] = f"PAGAMENTO A FORNECEDORES {company_name} ({cnpj_sem_zeros})"
                    elif transaction['type'] == 'PIX RECEBIDO':
                        transaction['description'] = f"PIX RECEBIDO {company_name} ({cnpj_sem_zeros})"
                    elif transaction['type'] == 'TED RECEBIDA':
                        transaction['description'] = f"TED RECEBIDA {company_name} ({cnpj_sem_zeros})"
                    transaction['has_company_info'] = True
        
        transactions.append(transaction)
    
    conn.close()
    return render_template('recebidos.html', 
                         transactions=transactions, 
                         totals=totals, 
                         tipo_filtro=tipo_filtro,
                         cnpj_filtro=cnpj_filtro,
                         start_date=start_date,
                         end_date=end_date,
                         cnpjs=cnpjs,
                         failed_cnpjs=len(failed_cnpjs))

@app.route('/enviados')
@login_required
def enviados():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get filters
    tipo_filtro = request.args.get('tipo', 'todos')
    cnpj_filtro = request.args.get('cnpj', 'todos')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    # Initialize totals dictionary
    totals = {
        'juros': 0.0,
        'iof': 0.0,
        'cartao': 0.0,
        'compensacao': 0.0,
        'aplicacao': 0.0,
        'cheque': 0.0,
        'multa': 0.0,
        'cancelamento': 0.0,
        'pix_enviado': 0.0,
        'ted_enviada': 0.0,
        'pagamento': 0.0
    }

    # Type mapping for totals
    type_mapping = {
        'JUROS': 'juros',
        'IOF': 'iof',
        'COMPRA CARTAO': 'cartao',
        'COMPENSACAO': 'compensacao',
        'APLICACAO': 'aplicacao',
        'CHEQUE EMITIDO/DEBITADO': 'cheque',
        'MULTA': 'multa',
        'CANCELAMENTO RESGATE': 'cancelamento',
        'PIX ENVIADO': 'pix_enviado',
        'TED ENVIADA': 'ted_enviada',
        'PAGAMENTO': 'pagamento'
    }

    # Base query
    query = '''
        SELECT date, description, value, type, document
        FROM transactions
        WHERE type IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    params = list(type_mapping.keys())

    # Add filters
    if tipo_filtro != 'todos':
        query += " AND type = ?"
        params.append(tipo_filtro)
    
    if cnpj_filtro != 'todos':
        query += " AND document = ?"
        params.append(cnpj_filtro)
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date DESC"
    cursor.execute(query, params)
    
    transactions = []
    for row in cursor.fetchall():
        transaction = {
            'date': row[0],
            'description': row[1],
            'value': float(row[2]),
            'type': row[3],
            'document': row[4],
            'has_company_info': False
        }
        
        # Update totals
        total_key = type_mapping.get(transaction['type'])
        if total_key:
            totals[total_key] += abs(transaction['value'])

        # Format description for CARTAO
        if transaction['type'] == 'COMPRA CARTAO':
            transaction['description'] = f"CARTÃO {transaction['description']}"
        
        # Get company info
        if transaction['document']:
            company_info = get_company_info(transaction['document'])
            if company_info:
                company_name = company_info.get('nome_fantasia') or company_info.get('razao_social', '')
                if company_name:
                    cnpj_sem_zeros = str(int(transaction['document']))
                    transaction['description'] = f"{transaction['type']} {company_name} ({cnpj_sem_zeros})"
                    transaction['has_company_info'] = True
        
        transactions.append(transaction)
    
    conn.close()
    return render_template('enviados.html',
                         transactions=transactions,
                         totals=totals,
                         tipo_filtro=tipo_filtro,
                         cnpj_filtro=cnpj_filtro,
                         start_date=start_date,
                         end_date=end_date,
                         failed_cnpjs=len(failed_cnpjs))

@app.route('/retry-failed-cnpjs')
@login_required
def retry_failed_cnpjs():
    return render_template('retry_cnpjs.html', active_page='retry_cnpjs')

@app.route('/retry-failed-cnpjs', methods=['POST'])
@login_required
def retry_failed_cnpjs_post():
    # POST request - retry failed CNPJs
    success_count = 0
    still_failed = set()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for cnpj in failed_cnpjs.copy():
            try:
                # Handle 15-digit CNPJ
                api_cnpj = cnpj
                if len(cnpj) == 15 and cnpj.startswith('0'):
                    api_cnpj = cnpj[1:]  # Remove first zero only if 15 digits
                
                response = requests.get(f'https://brasilapi.com.br/api/cnpj/v1/{api_cnpj}', timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    cnpj_cache[cnpj] = data
                    
                    # Atualiza as descrições no banco de dados
                    cursor.execute('''
                        SELECT id, description FROM transactions 
                        WHERE description LIKE ?
                    ''', (f'%{cnpj}%',))
                    
                    rows = cursor.fetchall()
                    for row in rows:
                        transaction_id, description = row
                        new_description = description.replace(cnpj, f"{data['razao_social']} (CNPJ: {cnpj})")
                        cursor.execute('''
                            UPDATE transactions 
                            SET description = ? 
                            WHERE id = ?
                        ''', (new_description, transaction_id))
                    
                    success_count += 1
                else:
                    still_failed.add(cnpj)
                    print(f"Falha ao buscar CNPJ {api_cnpj}: Status {response.status_code}")
            except Exception as e:
                still_failed.add(cnpj)
                print(f"Erro ao processar CNPJ {api_cnpj}: {str(e)}")
            
            # Pequena pausa entre requisições para evitar rate limit
            time.sleep(0.5)
        
        # Commit as alterações
        conn.commit()
        
        # Atualiza o conjunto de CNPJs que falharam
        failed_cnpjs.clear()
        failed_cnpjs.update(still_failed)
        
        return jsonify({
            'success': True,
            'message': f'Retry concluído. {success_count} CNPJs recuperados. {len(still_failed)} ainda com falha.',
            'failed_cnpjs': list(still_failed)
        })
    
    except Exception as e:
        print(f"Erro geral no retry: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erro ao processar retry: {str(e)}'
        }), 500
    
    finally:
        conn.close()

@app.route('/transactions-summary')
@login_required
def transactions_summary():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get transactions grouped by type, excluding PIX RECEBIDO, TED RECEBIDA, and PAGAMENTO
    cursor.execute('''
        SELECT 
            type,
            COUNT(*) as count,
            SUM(value) as total,
            GROUP_CONCAT(description || ' (' || value || ')') as details
        FROM transactions 
        WHERE type NOT IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO')
        GROUP BY type
        ORDER BY type, date DESC
    ''')
    
    summary = {}
    for row in cursor.fetchall():
        summary[row['type']] = {
            'count': row['count'],
            'total': row['total'],
            'details': row['details'].split(',') if row['details'] else []
        }
    
    conn.close()
    
    return render_template('transactions_summary.html', 
                         active_page='transactions_summary',
                         summary=summary)

@app.route('/verify-cnpj', methods=['POST'])
@login_required
def cnpj_verification():
    return render_template('cnpj_verification.html')

@app.route('/verify-cnpj/<cnpj>')
@login_required
def verify_cnpj(cnpj):
    """Verifica se um CNPJ é válido e retorna informações da empresa"""
    try:
        company_info = get_company_info(cnpj)
        if company_info:
            return jsonify({
                'valid': True,
                'company_name': company_info.get('nome_fantasia') or company_info.get('razao_social', ''),
                'cnpj': cnpj
            })
    except Exception as e:
        print(f"Erro ao verificar CNPJ {cnpj}: {e}")
    
    return jsonify({'valid': False, 'cnpj': cnpj})

@app.route('/cnpj-verification')
@login_required
def cnpj_verification_page():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    return render_template('cnpj_verification.html', active_page='cnpj_verification')

def extract_and_enrich_cnpj(description, transaction_type):
    # Find sequence of 14 digits that could be a CNPJ
    import re
    
    # Only process PIX RECEBIDO, TED RECEBIDA, and PAGAMENTO
    if transaction_type not in ['PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO']:
        return description
    
    # Try different CNPJ patterns
    cnpj_patterns = [
        r'CNPJ[:\s]*(\d{14,15})',  # CNPJ followed by 14 or 15 digits
        r'CNPJ[:\s]*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})',  # CNPJ followed by formatted number
        r'\b(\d{14,15})\b',  # Just 14 or 15 digits
        r'\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b'  # Formatted CNPJ
    ]
    
    cnpj_match = None
    for pattern in cnpj_patterns:
        match = re.search(pattern, description)
        if match:
            cnpj_match = match
            break
    
    if not cnpj_match:
        return description
        
    # Extract CNPJ and handle 15-digit case
    cnpj = ''.join(filter(str.isdigit, cnpj_match.group(1)))
    if len(cnpj) == 15 and cnpj.startswith('0'):
        cnpj = cnpj[1:]  # Remove first zero only if 15 digits
    elif len(cnpj) != 14:
        return description  # Invalid CNPJ length
    
    try:
        if cnpj in cnpj_cache:
            data = cnpj_cache[cnpj]
            razao_social = data.get('razao_social', '')
            new_description = description.replace(cnpj_match.group(0), f"{razao_social} (CNPJ: {cnpj})")
            return new_description
            
        response = requests.get(f'https://brasilapi.com.br/api/cnpj/v1/{cnpj}', timeout=5)
        if response.status_code == 200:
            data = response.json()
            cnpj_cache[cnpj] = data
            razao_social = data.get('razao_social', '')
            new_description = description.replace(cnpj_match.group(0), f"{razao_social} (CNPJ: {cnpj})")
            return new_description
        else:
            failed_cnpjs.add(cnpj)
    except Exception as e:
        print(f"Erro ao buscar CNPJ {cnpj}: {e}")
        failed_cnpjs.add(cnpj)
    
    return description

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5002))
    app.run(host='0.0.0.0', port=port, debug=False)
