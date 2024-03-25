from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import re
import io
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from azure.storage.blob import BlobServiceClient
from datetime import date
from pathlib import Path

# Variaveis de conexao e busca
load_dotenv()
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
CONNECT_STR = os.getenv('CONNECT_STR')
SCRIPT_FILE_PATH = os.getenv('SCRIPT_FILE_PATH')

lib_path = f'{SCRIPT_FILE_PATH}/lib.py'


options = webdriver.ChromeOptions()
#options.add_argument("--headless=new")

with open(lib_path, 'r') as script_file:
    script_code = script_file.read()
    exec(script_code)

current_date = date.today()

categorias  = [
    ('pneus-de-otr', 'niveladora'),
    ('pneus-de-otr', 'carregadeira'),
    ('pneus-agricolas', 'colheitadeira'),
    ('pneus-agricolas', 'maquina-compacta'),
    ('pneus-agricolas', 'pulverizador'),
    ('pneus-agricolas', 'tratores')
]

# Colunas do dataframe final
column_data_types = {
    'DAT_FATO': str,
    'DES_PRODUTO': str,
    'COD_PRODUTO': str,
    'DES_CATEGORIA': str,
    'DES_MARCA': str,
    'VAL_PRECO': float,
    'DES_CONDICOES': str,
    'DES_FONTE': str,
    'DES_APLICACAO': str
}

def get_product_brand(des_produto) -> str:
    try:
        r = des_produto[des_produto.find('Pneu',0,len(des_produto))+1:des_produto.find('Aro',0,len(des_produto))]
    except:
        r = ''
    return r

def get_data(soup, categoria):
    product_list = []

    for item in soup.findAll('div', class_='area-item'):
        
        dat_fato = current_date
        des_produto = item.findAll('a', class_='cx-product-name style-font')[0].text.strip()
        cod_produto = item.findAll('label', class_='product-id')[0].text.strip().replace("ID: ", "")
        des_categoria = categoria[0] + '-' +  categoria[1]
        des_marca = get_product_brand(des_produto)

        try:
            price = float(item.findAll('label', class_='label-price')[0].text.replace(" R$ ", "").replace("  à vista", "").replace('.','').replace(',','.'))
        except:
            price = 0.0

        try:
            installment = item.findAll('label', class_='price-parceled')[0].text.strip().replace("ou ", "")
        except:
            installment = ''

        source = 'SiteX'
        aplicacao = '-'

        product_list.append((dat_fato, des_produto, cod_produto, des_categoria, des_marca, price, installment, source, aplicacao))

    df = pd.DataFrame(data=product_list, columns=column_data_types.keys())
    return df


def scrap_SiteX(categorias: list):
    
    final_df = None
    
    url = f'https://www.SiteX.com.br/login'
    browser = webdriver.Chrome(options=options)
    browser.get(url)
    sleep(5)
    input_email = browser.find_element(by = By.ID, value="email")
    input_email.send_keys("precos@rech.com")
    sleep(2)
    input_senha = browser.find_element(by = By.ID, value="password")
    input_senha.send_keys("Rech2025@")
    sleep(2)
    button_login = browser.find_element(by = By.XPATH , value=f'//*[@id="CustomLogin"]/div/div/div/app-custom-auth-form/div/form/button')
    button_login.click()
    sleep(2)
    for categoria in categorias:

        url = f'https://www.SiteX.com.br/categorias/{categoria[0]}/{categoria[1]}'
        browser.get(url)
        sleep(5)
        keep_scrapping = True
        index = 1
        while keep_scrapping:

                page_content = browser.page_source
                soup = BeautifulSoup(page_content, "html.parser")

                elements = browser.find_elements(by=By.CLASS_NAME, value= "page")
                maximo = 0
                for e in elements:
                    if int(e.text) > maximo:
                        maximo = int(e.text)
                df = get_data(soup=soup, categoria=categoria)
                if final_df is None:
                    final_df = df
                else:
                    final_df = pd.concat([final_df, df])
                try:
                    newlink = f'https://www.SiteX.com.br/categorias/{categoria[0]}/{categoria[1]}?currentPage={str(index)}'
                    browser.get(newlink)
                    sleep(5)
                    index += 1
                except:
                    keep_scrapping = False
                if index > maximo:
                    keep_scrapping = False
    return final_df

# Executa webscraping
df = scrap_SiteX(categorias)
if not df.empty:
    df['VAL_PRECO'] = df['VAL_PRECO'].astype(float)
    df['DAT_FATO'] = pd.to_datetime(df['DAT_FATO']).dt.strftime('%Y-%m-%d')
    df.to_excel('D:/Webscrapping/SiteXscrapping.xlsx', index = False)

print('Processo finalizado...')