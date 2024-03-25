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

categorias = ['rodantes', 'fps','bordas', 'roletes']
current_date = date.today()

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


def get_product_code(des_produto) -> str:
    try:
        r = re.search(r'\(Cod\. (.*?)\)', des_produto).group(1)
    except:
        r = ''
    return r

def get_product_brand(des_produto) -> str:
    try:
        r = des_produto[des_produto.find(' -',0,len(des_produto))+1:des_produto.find('(Cod',0,len(des_produto))]
    except:
        r = ''
    return r

def get_data(soup, categoria, webD, oldUrl):
    product_list = []

    for item in soup.findAll('div', class_='product details product-item-details'):

        dat_fato = current_date
        des_produto = item.findAll('strong', class_='product name product-item-name')[0].text.strip()
        cod_produto = get_product_code(des_produto)
        des_marca = get_product_brand(des_produto)
        des_categoria = categoria

        try:
            price = get_standard_number(item.findAll('span', class_='price')[0].text)
        except:
            price = 0.0

        try:
            installment = item.findAll('span', class_='additional-price additional-price-installments')[0].text.strip()
        except:
            installment = ''

        source = 'SiteX'
        link = item.find_all("a", class_ = 'product-item-link')[0]
        new_url = link.get("href")
        webD.get(new_url)
        aplicacao = getAplicacao(BeautifulSoup(webD.page_source, "html.parser"))
        webD.get(oldUrl)
        product_list.append((dat_fato, des_produto, cod_produto, des_categoria, des_marca, price, installment, source, aplicacao))

    df = pd.DataFrame(data=product_list, columns=column_data_types.keys())
    return df

def getAplicacao(soup):
    aplicacao = ''
    try:
        teste = soup.findAll('div', class_='col-xs-12 col-md')[0]
        for li in teste.find_all("li"): 
            aplicacao  = aplicacao + '/' + li.text.strip()
    except:
        aplicacao = '-'
    return aplicacao

def scrap_SiteX(categorias: list):
    
    final_df = None

    for categoria in categorias:

        url = f'https://lojaSiteX.com.br/catalogsearch/result/?q={categoria}'
        browser = webdriver.Chrome(options=options)
        browser.get(url)
        sleep(5)
        keep_scrapping = True

        while keep_scrapping:

                page_content = browser.page_source                
                soup = BeautifulSoup(page_content, "html.parser")
                df = get_data(soup=soup, categoria=categoria, webD = browser, oldUrl = browser.current_url)

                if final_df is None:
                    final_df = df
                else:
                    final_df = pd.concat([final_df, df])

                try:
                    next_button = browser.find_element(By.CSS_SELECTOR, 'li.item.pages-item-next')
                    next_button.click()
                    sleep(5)
                except:
                    keep_scrapping = False
                    browser.quit()
    
    return final_df

# Executa webscraping
df = scrap_SiteX(categorias)

if not df.empty:
    df['VAL_PRECO'] = df['VAL_PRECO'].astype(float)
    df['DAT_FATO'] = pd.to_datetime(df['DAT_FATO']).dt.strftime('%Y-%m-%d')

    # Grava arquivo no lake
    df.to_excel('D:/Webscrapping/SiteX.xlsx', index = False)

print('Processo finalizado...')

