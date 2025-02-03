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
from readers.santander import SantanderReader
from readers.itau import ItauReader
import re

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=1)  # Set session lifetime to 1 hour

# Global variables
upload_progress = {}  # Dictionary to track file upload progress
cnpj_cache = {}  # Cache for storing company information 
failed_cnpjs = set()  # Set for storing failed CNPJs

AF_COMPANIES = {
    '50389827000107': 'AF ENERGY SOLAR 360',
    '43077430000114': 'AF 360 CORRETORA DE SEGUROS LTDA',
    '53720093000195': 'AF CREDITO BANK',
    '55072511000100': 'AF COMERCIO DE CALCADOS LTDA',
    '17814862000150': 'AF 360 FRANQUIAS LTDA'
}

PRIMARY_TYPES = ['PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO']

TYPE_MAPPING = {
    'APLICACAO': 'CONTAMAX',
    'RESGATE': 'CONTAMAX',
    'COMPENSACAO': 'CHEQUE',
    'COMPRA': 'CARTAO',
    'TAXA': 'DESPESAS OPERACIONAIS',
    'TARIFA': 'DESPESAS OPERACIONAIS',
    'IOF': 'DESPESAS OPERACIONAIS',
    'MULTA': 'DESPESAS OPERACIONAIS',
    'DEBITO': 'DESPESAS OPERACIONAIS',
}

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
    """Fetch company information using cache if available"""
    # Check if already in cache
    if cnpj in cnpj_cache:
        return cnpj_cache[cnpj]
    
    try:
        response = requests.get(f'https://brasilapi.com.br/api/cnpj/v1/{cnpj}')
        if response.status_code == 200:
            company_info = response.json()
            # Store in cache
            cnpj_cache[cnpj] = company_info
            if cnpj in failed_cnpjs:
                failed_cnpjs.remove(cnpj)
            return company_info
        else:
            failed_cnpjs.add(cnpj)
    except Exception as e:
        print(f"Error fetching company information: {e}")
        failed_cnpjs.add(cnpj)
    return None

def format_company_info(company_info, cnpj):
    """Format company info for display"""
    return {
        'cnpj': cnpj,
        'nome_fantasia': company_info.get('nome_fantasia', ''),
        'razao_social': company_info.get('razao_social', ''),
        'formatted_name': (
            company_info.get('nome_fantasia') or 
            company_info.get('razao_social', '')
        ) + f" (CNPJ: {cnpj})"
    }

def is_af_company_transaction(description):
    """Check if transaction description contains an AF company name"""
    return any(company_name.upper() in description.upper() for company_name in AF_COMPANIES.values())

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
            return jsonify({'success': False, 'message': 'Nenhum arquivo selecionado'})
        
        file = request.files['file']
        bank_type = request.form.get('bank_type')
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'Nenhum arquivo selecionado'})
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Initialize progress
            process_id = str(uuid.uuid4())
            upload_progress[process_id] = {
                'status': 'processing',
                'current': 0,
                'total': 0,
                'message': 'Iniciando processamento...'
            }
            
            # Select reader based on bank type
            if bank_type == 'santander':
                reader = SantanderReader()
            elif bank_type == 'itau':
                reader = ItauReader()
            else:
                return jsonify({'success': False, 'message': 'Banco não suportado'})
            
            # Process file in separate thread
            thread = threading.Thread(
                target=reader.process_file, 
                args=(filepath, process_id, upload_progress)
            )
            thread.start()
            
            return jsonify({
                'success': True,
                'process_id': process_id,
                'message': 'Arquivo enviado e sendo processado'
            })
        
        return jsonify({'success': False, 'message': 'Tipo de arquivo não permitido'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Erro ao processar arquivo: {str(e)}'})

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
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Read Excel without header first
        df = pd.read_excel(filepath, header=None)
        
        # Find actual data start
        data_start_row = 0
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and isinstance(row[0], str):
                if 'AGENCIA' in row[0].upper() or 'DATA' in row[0].upper():
                    data_start_row = idx + 1
                    break
        
        # Read file again with proper header row
        df = pd.read_excel(filepath, skiprows=data_start_row)
        total_rows = len(df)
        
        upload_progress[process_id].update({
            'total': total_rows,
            'message': 'Processing file...'
        })

        # Fixed totals with all possible types
        totals = {
            'pix_recebido': 0.0,
            'ted_recebida': 0.0,
            'pagamento': 0.0,
            'cheque': 0.0,
            'contamax': 0.0,
            'diversos': 0.0,
            'despesas_operacionais': 0.0,
            'taxa': 0.0,
            'tarifa': 0.0,
            'iof': 0.0,
            'multa': 0.0,
            'debito': 0.0,
            'juros': 0.0
        }

        # Process each row
        for index, row in df.iterrows():
            try:
                # Safe date parsing
                date_str = str(row.iloc[0])
                try:
                    date = pd.to_datetime(date_str, format='%d/%m/%Y').date()
                except:
                    date = pd.to_datetime(date_str, dayfirst=True).date()
                
                description = str(row.iloc[1]).strip()
                value = float(str(row.iloc[2]).replace('R$', '').strip().replace('.', '').replace(',', '.'))
                
                # Get transaction type
                transaction_type = detect_transaction_type(description, value)
                
                # Insert transaction
                cursor.execute('''
                    INSERT INTO transactions (date, description, value, type, transaction_type, document)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    date.strftime('%Y-%m-%d'),
                    description,
                    value,
                    transaction_type,
                    'receita' if value > 0 else 'despesa',
                    None
                ))
                
                upload_progress[process_id]['current'] = index + 1
                
            except Exception as e:
                print(f"Error processing row {index}: {str(e)}")
                continue

        conn.commit()
        conn.close()
        os.remove(filepath)
        
        upload_progress[process_id].update({
            'status': 'completed',
            'message': 'File processed successfully'
        })
        
    except Exception as e:
        upload_progress[process_id].update({
            'status': 'error',
            'message': f'Error: {str(e)}'
        })
def detect_transaction_type(description, value):
    description_upper = description.upper()
    
    # Primary types
    if 'PIX' in description_upper:
        return 'PIX RECEBIDO' if value > 0 else 'PIX ENVIADO'
    elif 'TED' in description_upper:
        return 'TED RECEBIDA' if value > 0 else 'TED ENVIADA'
        
    # Secondary types
    for tipo, keywords in {
        'TARIFA': ['TARIFA', 'TAR'],
        'IOF': ['IOF'],
        'RESGATE': ['RESGATE'],
        'APLICACAO': ['APLICACAO', 'APLICAÇÃO'],
        'COMPRA': ['COMPRA'],
        'COMPENSACAO': ['COMPENSACAO', 'COMPENSAÇÃO'],
        'CHEQUE': ['CHEQUE'],
        'JUROS': ['JUROS'],
        'MULTA': ['MULTA']
    }.items():
        if any(keyword in description_upper for keyword in keywords):
            return tipo
            
    return 'DIVERSOS' if value > 0 else 'DEBITO'

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

def create_companies_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            document TEXT PRIMARY KEY,
            nome_fantasia TEXT,
            razao_social TEXT
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/recebidos')
@login_required
def recebidos():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get filters
    tipo_filtro = request.args.get('tipo', 'todos')
    cnpj_filtro = request.args.get('cnpj', 'todos')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    # Initialize all possible totals
    totals = {
        'pix_recebido': 0.0,
        'ted_recebida': 0.0,
        'pagamento': 0.0,
        'cheque': 0.0,
        'contamax': 0.0,
        'juros': 0.0,
        'despesas_operacionais': 0.0,
        'diversos': 0.0,
        'taxa': 0.0,
        'tarifa': 0.0,
        'iof': 0.0,
        'multa': 0.0,
        'debito': 0.0
    }

    # Base query
    query = '''
        SELECT t.id, t.date, t.description, t.value,
               t.type AS original_type,
               CASE
                   WHEN t.type IN ('APLICACAO', 'RESGATE') THEN 'CONTAMAX'
                   WHEN t.type IN ('COMPENSACAO', 'CHEQUE') THEN 'CHEQUE'
                   WHEN t.type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO') THEN 'DESPESAS OPERACIONAIS'
                   WHEN t.type IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO') THEN t.type
                   ELSE 'DIVERSOS'
               END AS displayed_type,
               t.document
        FROM transactions t
        WHERE t.value > 0
    '''

    # Build query with filters
    params = []
    if tipo_filtro != 'todos':
        if tipo_filtro == 'DIVERSOS':
            query += " AND t.type NOT IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO')"
        elif tipo_filtro == 'CHEQUE':
            query += " AND t.type IN ('CHEQUE', 'COMPENSACAO')"
        elif tipo_filtro == 'CONTAMAX':
            query += " AND t.type IN ('APLICACAO', 'RESGATE')"
        elif tipo_filtro == 'DESPESAS OPERACIONAIS':
            query += " AND t.type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO')"
        elif tipo_filtro in PRIMARY_TYPES:
            query += " AND t.type = ?"
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

    # Execute query
    query += " ORDER BY date DESC"
    cursor.execute(query, params)
    rows = cursor.fetchall()

    # Process transactions
    transactions = []
    for row in rows:
        original_type = row[4]
        mapped_type = row[5]  # Use the mapped type from query
        
        transaction = {
            'date': row[1],
            'description': row[2],
            'value': float(row[3]),
            'type': mapped_type,
            'original_type': original_type,
            'document': row[6],
            'has_company_info': False
        }
         
        # Update type-specific descriptions
        if transaction['type'] == 'CHEQUE':
            transaction['description'] = f"CHEQUE - {transaction['description']}"
        elif transaction['type'] == 'CONTAMAX':
            transaction['description'] = f"CONTAMAX - {transaction['description']}"
        elif transaction['type'] == 'DESPESAS OPERACIONAIS':
            transaction['description'] = f"DESPESAS - {transaction['description']}"
            
        # Update totals
        type_key = transaction['type'].lower().replace(' ', '_')
        if type_key in totals:
            totals[type_key] += transaction['value']
        else:
            totals['diversos'] += transaction['value']
            
    # Process transactions and update totals
    transactions = []
    for row in rows:
        value = float(row[3])
        original_type = row[4].lower().replace(' ', '_')
        displayed_type = row[5].lower().replace(' ', '_')
        
        # Update totals for both original and displayed types
        if original_type in totals:
            totals[original_type] += value
        if displayed_type in totals:
            totals[displayed_type] += value
            
        transaction = {
            'date': row[1],
            'description': row[2],
            'value': value,
            'type': row[5],
            'original_type': row[4],
            'document': row[6],
            'has_company_info': False
        }
        transactions.append(transaction)

    # Get unique CNPJs for dropdown
    cnpjs = [
        {'cnpj': cnpj, 'name': info.get('nome_fantasia') or info.get('razao_social', '')} 
        for cnpj, info in cnpj_cache.items() 
        if cnpj not in AF_COMPANIES
    ]

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
    conn = get_db_connection()
    cursor = conn.cursor()

    # Initialize totals
    totals = {
        'pix_enviado': 0.0,
        'ted_enviada': 0.0,
        'pagamento': 0.0,
        'cheque': 0.0,
        'contamax': 0.0,
        'despesas_operacionais': 0.0,
        'diversos': 0.0,
        'taxa': 0.0,
        'tarifa': 0.0,
        'iof': 0.0,
        'multa': 0.0,
        'debito': 0.0
    }

    # Get filters
    tipo_filtro = request.args.get('tipo', 'todos')
    cnpj_filtro = request.args.get('cnpj', 'todos')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    # Base query
    query = '''
        SELECT t.id, t.date, t.description, ABS(t.value) as value,
               t.type AS original_type,
               CASE
                   WHEN t.type IN ('APLICACAO', 'RESGATE') THEN 'CONTAMAX'
                   WHEN t.type IN ('COMPENSACAO', 'CHEQUE') THEN 'CHEQUE'
                   WHEN t.type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO') THEN 'DESPESAS OPERACIONAIS'
                   WHEN t.type IN ('PIX ENVIADO', 'TED ENVIADA', 'PAGAMENTO') THEN t.type
                   ELSE 'DIVERSOS'
               END AS displayed_type,
               t.document
        FROM transactions t
        WHERE t.value < 0
    '''

    # Build filters
    params = []
    if tipo_filtro != 'todos':
        if tipo_filtro == 'DIVERSOS':
            query += " AND t.type NOT IN ('PIX ENVIADO', 'TED ENVIADA', 'PAGAMENTO')"
        elif tipo_filtro == 'CHEQUE':
            query += " AND t.type IN ('CHEQUE', 'COMPENSACAO')"
        elif tipo_filtro == 'CONTAMAX':
            query += " AND t.type IN ('APLICACAO', 'RESGATE')"
        elif tipo_filtro == 'DESPESAS OPERACIONAIS':
            query += " AND t.type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO')"
        else:
            query += " AND t.type = ?"
            params.append(tipo_filtro)

    if cnpj_filtro != 'todos':
        query += " AND t.document = ?"
        params.append(cnpj_filtro)

    if start_date:
        query += " AND t.date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND t.date <= ?"
        params.append(end_date)

    # Add ordering
    query += " ORDER BY t.date DESC"

    try:
        # Execute query
        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Process transactions
        transactions = []
        for row in rows:
            value = float(row[3])
            transaction = {
                'date': row[1],
                'description': row[2],
                'value': value,
                'type': row[5],
                'original_type': row[4],
                'document': row[6],
                'has_company_info': False
            }

            # Update totals
            type_key = transaction['type'].lower().replace(' ', '_')
            if type_key in totals:
                totals[type_key] += value
            else:
                totals['diversos'] += value

            transactions.append(transaction)

        # Get unique CNPJs for dropdown
        cnpjs = [
            {'cnpj': cnpj, 'name': info.get('nome_fantasia') or info.get('razao_social', '')} 
            for cnpj, info in cnpj_cache.items() 
            if cnpj not in AF_COMPANIES
        ]

        return render_template('enviados.html',
                             transactions=transactions,
                             totals=totals,
                             tipo_filtro=tipo_filtro,
                             cnpj_filtro=cnpj_filtro,
                             start_date=start_date,
                             end_date=end_date,
                             cnpjs=cnpjs,
                             failed_cnpjs=len(failed_cnpjs))

    except Exception as e:
        print(f"Database error: {str(e)}")
        return render_template('error.html', error=str(e))
    finally:
        conn.close()

@app.route('/transacoes_internas')
@login_required
def transacoes_internas():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get filters
    tipo_filtro = request.args.get('tipo', 'todos')
    cnpj_filtro = request.args.get('cnpj', 'todos')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    # Initialize totals
    totals = {
        'juros': 0.0,
        'iof': 0.0,
        'pix_enviado': 0.0,
        'ted_enviada': 0.0,
        'pagamento': 0.0,
        'diversos': 0.0
    }

    # Initialize transactions list
    transactions = []

    # Modified query for internal transactions
    query = '''
        SELECT DISTINCT t1.date, t1.description, t1.value, t1.type, t1.document
        FROM transactions t1
        WHERE (
            t1.document IN ({af_companies})
            OR {conditions}
            OR t1.description LIKE '%AF%'
        )
        AND (
            t1.type LIKE '%PIX%'
            OR t1.type LIKE '%TED%'
            OR t1.type = 'PAGAMENTO'
        )
    '''.format(
        af_companies=','.join(['?' for _ in AF_COMPANIES]),
        conditions=' OR '.join([
            "t1.description LIKE ?"
            for _ in AF_COMPANIES.values()
        ])
    )
    
    # Add parameters
    params = list(AF_COMPANIES.keys())
    params.extend(['%' + name + '%' for name in AF_COMPANIES.values()])

    # Add filters
    if tipo_filtro != 'todos':
        query += " AND t1.type = ?"
        params.append(tipo_filtro)

    if cnpj_filtro != 'todos':
        query += " AND (t1.document = ? OR t1.description LIKE ?)"
        params.extend([cnpj_filtro, '%' + AF_COMPANIES.get(cnpj_filtro, '') + '%'])

    if start_date:
        query += " AND t1.date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND t1.date <= ?"
        params.append(end_date)

    query += " ORDER BY t1.date DESC"
    
    # Execute query
    cursor.execute(query, params)
    rows = cursor.fetchall()  # Store rows here
    
    # Process results
    transactions = []
    for row in rows:  # Use stored rows
        transaction = {
            'date': row[0],
            'description': row[1],
            'value': float(row[2]),
            'type': row[3] if row[3] else 'DIVERSOS',
            'document': row[4],
            'has_company_info': True
        }

        # Improved company name detection
        company_name = None
        if transaction['document'] in AF_COMPANIES:
            company_name = AF_COMPANIES[transaction['document']]
        else:
            for name in AF_COMPANIES.values():
                if name.upper() in transaction['description'].upper():
                    company_name = name
                    break

        if company_name:
            transaction['description'] = f"{transaction['type']} - {company_name}"

        transactions.append(transaction)

    # Create CNPJs list only with AF companies
    cnpjs = [{'cnpj': cnpj, 'name': name} for cnpj, name in AF_COMPANIES.items()]

    return render_template('transacoes_internas.html',
                         transactions=transactions,
                         totals=totals,
                         tipo_filtro=tipo_filtro,
                         cnpj_filtro=cnpj_filtro,
                         start_date=start_date,
                         end_date=end_date,
                         cnpjs=cnpjs,
                         failed_cnpjs=0)

@app.route('/dashboard')
@login_required
def dashboard():
    if not session.get('authenticated'):
        return redirect('https://af360bank.onrender.com/login')
    
    conn = get_db_connection()
    cursor = conn.cursor()

    # Calculate totals
    cursor.execute('''
        SELECT 
            (SELECT COALESCE(SUM(value), 0) FROM transactions WHERE value > 0) as total_received,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE value < 0) as total_sent,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type = 'JUROS') as juros,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type = 'IOF') as iof,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type IN ('TARIFA', 'TAR', 'TAXA')) as tarifa,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type = 'MULTA') as multa,
            (SELECT COALESCE(SUM(value), 0) FROM transactions WHERE type = 'PIX RECEBIDO') as pix_recebido,
            (SELECT COALESCE(SUM(value), 0) FROM transactions WHERE type = 'TED RECEBIDA') as ted_recebida,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type = 'PIX ENVIADO') as pix_enviado,
            (SELECT COALESCE(SUM(ABS(value)), 0) FROM transactions WHERE type = 'TED ENVIADA') as ted_enviada
    ''')
    
    row = cursor.fetchone()
    totals = {
        'recebidos': float(row[0] or 0),
        'enviados': float(row[1] or 0),
        'juros': float(row[2] or 0),
        'iof': float(row[3] or 0),
        'tarifa': float(row[4] or 0),
        'multa': float(row[5] or 0),
        'pix_recebido': float(row[4] or 0),
        'ted_recebida': float(row[5] or 0),
        'pix_enviado': float(row[6] or 0),
        'ted_enviada': float(row[7] or 0)
    }

    # Get monthly data for cash flow
    cursor.execute('''
        SELECT 
            strftime('%m/%Y', date) as month,
            COALESCE(SUM(CASE WHEN value > 0 THEN value ELSE 0 END), 0) as received,
            COALESCE(SUM(CASE WHEN value < 0 THEN ABS(value) ELSE 0 END), 0) as sent
        FROM transactions
        GROUP BY month
        ORDER BY date ASC
        LIMIT 6
    ''')
    
    months = []
    received = []
    sent = []
    for row in cursor.fetchall():
        months.append(row[0])
        received.append(float(row[1] or 0))
        sent.append(float(row[2] or 0))

    # Get expenses distribution
    cursor.execute('''
        SELECT 
            type,
            COALESCE(SUM(ABS(value)), 0) as total
        FROM transactions
        WHERE value < 0
        GROUP BY type
        ORDER BY total DESC
    ''')
    
    expense_types = []
    expense_values = []
    for row in cursor.fetchall():
        expense_types.append(row[0])
        expense_values.append(float(row[1] or 0))

    # Get top CNPJs with names
    cursor.execute('''
        SELECT 
            document,
            COALESCE(SUM(ABS(value)), 0) as total
        FROM transactions
        WHERE document IS NOT NULL
        GROUP BY document
        ORDER BY total DESC
        LIMIT 5
    ''')
    
    top_cnpjs = []
    for row in cursor.fetchall():
        if row[0]:
            company_info = get_company_info(row[0])
            if company_info:
                name = company_info.get('nome_fantasia') or company_info.get('razao_social', '')
                if name:
                    top_cnpjs.append({
                        'name': name,
                        'value': float(row[1] or 0)
                    })

    conn.close()

    return render_template('dashboard.html',
                         active_page='dashboard',
                         totals=totals,
                         months=months,
                         received=received,
                         sent=sent,
                         expense_types=expense_types,
                         expense_values=expense_values,
                         top_cnpjs=top_cnpjs)

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
    
    # Updated query to include all transaction types
    cursor.execute("""
        SELECT 
            CASE
                WHEN type IN ('APLICACAO', 'RESGATE') THEN 'CONTAMAX'
                WHEN type = 'COMPENSACAO' OR type = 'CHEQUE' THEN 'CHEQUE'
                WHEN type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO') THEN 'DESPESAS OPERACIONAIS'
                ELSE type
            END as type,
            COUNT(*) as count,
            SUM(value) as total,
            GROUP_CONCAT(description || ' (' || value || ')') as details
        FROM transactions 
        GROUP BY 
            CASE
                WHEN type IN ('APLICACAO', 'RESGATE') THEN 'CONTAMAX'
                WHEN type = 'COMPENSACAO' OR type = 'CHEQUE' THEN 'CHEQUE'
                WHEN type IN ('TAXA', 'TARIFA', 'IOF', 'MULTA', 'DEBITO') THEN 'DESPESAS OPERACIONAIS'
                ELSE type
            END
        ORDER BY 
            CASE 
                WHEN type IN ('PIX RECEBIDO', 'TED RECEBIDA', 'PAGAMENTO') THEN 1
                ELSE 2
            END,
            ABS(SUM(value)) DESC
    """)
    
    summary = {}
    for row in cursor.fetchall():
        summary[row[0]] = {
            'count': row[1],
            'total': row[2],
            'details': row[3].split(',') if row[3] else []
        }
    
    conn.close()
    
    return render_template('transactions_summary.html', 
                         active_page='transactions_summary',
                         summary=summary)

@app.route('/verify-cnpj', methods=['GET', 'POST'])
@login_required
def cnpj_verification():
    if request.method == 'POST':
        cnpj = request.form.get('cnpj')
        return redirect(url_for('verify_cnpj', cnpj=cnpj))
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
        return jsonify({'valid': False, 'error': str(e), 'cnpj': cnpj})
    
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