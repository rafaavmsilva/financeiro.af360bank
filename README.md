# Sistema Financeiro

Um sistema web para leitura de extratos bancarios e validação de CNPJ.Desenvolvido com Flask e SQLite.

## Funcionalidades

- Registro de receitas e despesas
- Verificação e identificação de CNPJs pela receita federal
- Histórico de transações
- Armazenamento em banco de dados do SQLite

## Requisitos

- Python 3.7+
- Flask
- SQLite3

## Instalação (Necessário Remover os Códigos de Verificação de token)

1. Clone o repositório ou baixe os arquivos
2. Instale as dependências:
```bash
pip install flask
```

3. Execute o aplicativo:
```bash
python app.py
```

4. Acesse o sistema no navegador:
```
http://localhost:5000
```

## Estrutura do Projeto

```
projeto_financeiro/
├── app.py              # Aplicação Flask principal
├── instance/          # Banco de dados SQLite
├── static/
│   ├── css/
│   │   └── style.css  # Estilos personalizados
│   └── js/
│       └── script.js  # JavaScript do cliente
└── templates/
    └── index.html     # Página principal
```

## Uso

1. Acesse a página principal
2. Use o formulário para importar um extrato
3. Visualize o resumo financeiro e as inforamações dos CNPJs
4. Consulte o histórico de transações na tabela