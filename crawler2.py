import os
import psycopg2
import requests
import threading
import logging
import time
from bs4 import BeautifulSoup

# Configurar o logger para registrar informações e erros
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="aaa",
    password="aaa",
    host="localhost",
    port="5432"
)

current_directory = os.path.dirname(os.path.abspath(__file__))
image_directory = os.path.join(current_directory, "imagens")
os.makedirs(image_directory, exist_ok=True)

cursor = conn.cursor()

# Consultar as URLs da tabela books
cursor.execute("SELECT id, href FROM books WHERE descricao IS NULL AND formato IS NULL AND num_paginas IS NULL AND subtitulo IS NULL AND editora IS NULL AND autor IS NULL AND ano_edicao IS NULL AND ean13 IS NULL AND edicao IS NULL AND idioma IS NULL AND fabricante IS NULL AND isbn IS NULL AND paginas IS NULL")
urls = cursor.fetchall()

# Função para baixar uma imagem usando uma URL e salvar localmente
def download_image(url, filename, image_directory):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            # Salvar a imagem localmente com o nome definido pela variável 'filename'
            image_path = os.path.join(image_directory, f"{filename}.jpg")
            with open(image_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return image_path
        else:
            logging.error(f"Erro ao baixar a imagem - Status Code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Erro ao baixar a imagem: {e}")
        return None

# Função para extrair informações da página e baixar imagens usando threads
def process_url(id, url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        book_info = {}

        # Extrair o EAN13
        ean13_element = soup.find('td', class_='value-field EAN13')
        ean13 = ean13_element.text.strip() if ean13_element else None

        # Extrair a URL da imagem principal
        image_main = soup.find('img', {'id': 'image-main'})
        if image_main:
            filename = f"livro_{ean13}" if ean13 else "livro_unknown"  # Use o EAN13 como parte do nome do arquivo, se disponível
            image_path = download_image(image_main.get('src'), filename, image_directory)
            book_info['image_path'] = image_path
            book_info['image_url'] = image_main.get('src')
        else:
            book_info['image_url'] = None

        # Extrair outras informações com verificação de None
        formato_element = soup.find('td', class_='value-field Formato')
        book_info['formato'] = formato_element.text.strip() if formato_element else None

        num_paginas_element = soup.find('td', class_='value-field Numero-de-Paginas')
        book_info['num_paginas'] = num_paginas_element.text.strip() if num_paginas_element else None

        subtitulo_element = soup.find('td', class_='value-field Subtitulo')
        book_info['subtitulo'] = subtitulo_element.text.strip() if subtitulo_element else None

        editora_element = soup.find('td', class_='value-field Editora')
        book_info['editora'] = editora_element.text.strip() if editora_element else None

        autor_element = soup.find('td', class_='value-field Autor')
        book_info['autor'] = autor_element.text.strip() if autor_element else None

        ano_edicao_element = soup.find('td', class_='value-field Ano-da-Edicao')
        book_info['ano_edicao'] = ano_edicao_element.text.strip() if ano_edicao_element else None

        edicao_element = soup.find('td', class_='value-field Edicao')
        book_info['edicao'] = edicao_element.text.strip() if edicao_element else None

        idioma_element = soup.find('td', class_='value-field Idioma')
        book_info['idioma'] = idioma_element.text.strip() if idioma_element else None

        fabricante_element = soup.find('td', class_='value-field Fabricante')
        book_info['fabricante'] = fabricante_element.text.strip() if fabricante_element else None

        isbn_element = soup.find('td', class_='value-field ISBN')
        book_info['isbn'] = isbn_element.text.strip() if isbn_element else None

        paginas_element = soup.find('td', class_='value-field Paginas')
        book_info['paginas'] = paginas_element.text.strip() if paginas_element else None

        description_element = soup.find('div', class_='productDescription')
        book_info['descricao'] = description_element.text.strip() if description_element else None

        # Atualizar o banco de dados com as informações extraídas
        cursor.execute("UPDATE books SET descricao = %s, formato = %s, num_paginas = %s, subtitulo = %s, editora = %s, autor = %s, ano_edicao = %s, ean13 = %s, edicao = %s, idioma = %s, fabricante = %s, isbn = %s, paginas = %s, image_path = %s WHERE id = %s",
                       (book_info['descricao'], book_info['formato'], book_info['num_paginas'], book_info['subtitulo'], book_info['editora'],
                        book_info['autor'], book_info['ano_edicao'], ean13, book_info['edicao'],
                        book_info['idioma'], book_info['fabricante'], book_info['isbn'], book_info['paginas'], book_info['image_path'], id))
        conn.commit()

        logging.info(f'Informações extraídas da URL {url}: {book_info}')
    except Exception as e:
        logging.error(f'Erro ao extrair informações da URL {url}: {e}')
        conn.rollback()

# Lista para armazenar as threads
threads = []

# Iterar sobre as URLs e iniciar uma nova thread para cada URL
for id, url in urls:
    thread = threading.Thread(target=process_url, args=(id, url))
    threads.append(thread)
    thread.start()
    time.sleep(2)  # Aguarda 2 segundos entre as solicitações para evitar bloqueio

# Aguardar todas as threads terminarem
for thread in threads:
    thread.join()

# Fechar a conexão com o banco de dados
conn.close()
