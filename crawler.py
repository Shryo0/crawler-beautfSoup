import requests
from bs4 import BeautifulSoup
import psycopg2

# Função para extrair dados da página da Livraria Curitiba
def crawl_livraria_curitiba(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    products = []
    for product in soup.find_all('div', class_='product-id'):
        title = product.find('h3', class_='box-name').text.strip()
        price = product.find('div', class_='bestPrice').text.strip()
        href = product.find('a', class_='productImage')['href']
        products.append((title, price, href))  # Deixe as outras colunas vazias

    return products

# Função para salvar os dados no banco de dados PostgreSQL
def save_to_database(data):
    conn = psycopg2.connect(
        dbname="postgres",
        user="aaa",
        password="aaa",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Criar a tabela se ela não existir
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS books
    (id SERIAL PRIMARY KEY,
    title TEXT,
    price TEXT,
    href TEXT,
    formato VARCHAR(255),
    num_paginas INTEGER,
    subtitulo TEXT,
    editora VARCHAR(255),
    autor VARCHAR(255),
    ano_edicao INTEGER,
    ean13 VARCHAR(255),
    edicao INTEGER,
    idioma VARCHAR(255),
    fabricante VARCHAR(255),
    isbn VARCHAR(255),
    paginas INTEGER,
    image_path VARCHAR(255),
    descricao TEXT)
    ''')

    # Inserir os dados na tabela
    insert_query = '''
    INSERT INTO books (title, price, href)
    VALUES (%s, %s, %s)
    '''

    # Iterar sobre os dados e inserir na tabela
    for product_data in data:
        cursor.execute(insert_query, product_data)

    # Commit e fechar conexão
    conn.commit()
    conn.close()

# URL da página da Livraria Curitiba
url = 'https://www.livrariascuritiba.com.br/mais-vendidos?O=OrderByTopSaleDESC'

try:
    products_data = crawl_livraria_curitiba(url)
    save_to_database(products_data)
    print('Dados da página foram rastreados e salvos no banco de dados PostgreSQL.')
except Exception as e:
    print(f'Erro ao processar a página: {e}')
