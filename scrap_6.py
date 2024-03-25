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
options.add_argument("--headless=new")

with open(lib_path, 'r') as script_file:
    script_code = script_file.read()
    exec(script_code)

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


def get_data(soup, categoria):
    product_list = []
    
    for item in soup.findAll('div', class_='showcase__item'):
        
        dat_fato = current_date
        try:            
            des_produto = item.findAll('h3', class_='product__name')[0].text.strip()
        except:
            #print(item.findAll('h3', class_='product__name'))
            des_produto = ''

        try:            
            cod_produto = item.findAll('small', class_='product__reference')[0].text.strip()
        except:
            #print(item.findAll('small', class_='product__reference'))
            cod_produto = ''
        
        des_categoria = categoria

        try:
            
            price = float(item.findAll('span', class_='prices__price')[0].text.replace("R$", "").replace("  à vista", "").replace('.','').replace(',','.'))
        except:
            price = 0.0

        try:
            installment = item.findAll('span', class_='txt-corparcelas')[0].text.strip().replace("ou ", "")
            installment =  installment + ' ' +item.findAll('strong', class_='preco-parc1')[0].text.strip().replace("ou ", "")
        except:
            installment = ''
        marca = '-'
        aplicacao = '-'
        source = 'SiteX'
        if len(cod_produto) >  0 and len(des_produto) > 0:
            product_list.append((dat_fato, des_produto, cod_produto, des_categoria, marca, price, installment, source, aplicacao))

    df = pd.DataFrame(data=product_list, columns=column_data_types.keys())
    return df


def scrap_SiteX():
    
    final_df = None
    browser = webdriver.Chrome(options=options)
    url = f'https://loja.SiteX.com.br/f-p-s-ferramentas-de-penetracao-no-solo'
    browser.get(url)
    sleep(2)
    
    keep_scrapping = True
    index = 2
    while keep_scrapping:

            page_content = browser.page_source
            soup = BeautifulSoup(page_content, "html.parser")
            categoria = "Ferramentas de Penetração no Solo"
            elements = browser.find_elements(by=By.CLASS_NAME, value= "paginate__pages")
            maximo = 0
            for e in elements:
                maximo = int(max(e.text))     
                break      
            df = get_data(soup=soup, categoria=categoria)
            if final_df is None:
                final_df = df
            else:
                final_df = pd.concat([final_df, df])
            try:
                newlink = f'https://loja.SiteX.com.br/loja/catalogo.php?loja=1140109&categoria=3&pg={str(index)}'
                browser.get(newlink)
                sleep(5)
                index += 1
            except:
                keep_scrapping = False
            if index > maximo:
                keep_scrapping = False
    return final_df

# Executa webscraping
df = scrap_SiteX()
if not df.empty:
    df['VAL_PRECO'] = df['VAL_PRECO'].astype(float)
    df['DAT_FATO'] = pd.to_datetime(df['DAT_FATO']).dt.strftime('%Y-%m-%d')
    df.to_excel('D:/SiteXscrapping.xlsx', index = False)

print('Processo finalizado...')