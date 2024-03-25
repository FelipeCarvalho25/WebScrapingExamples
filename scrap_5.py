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
    ('Colheitadeiras', 'Correias para Colheitadeiras'),
    ('Colheitadeiras', 'Esteiras Draper para Colheitadeiras'),
    ('Colheitadeiras', 'Gerais para Colheitadeiras'),
    ('Colheitadeiras', 'Rolamentos'),
    ('Pulverizadores', 'Bicos'),
    ('Pulverizadores', 'Gerais para Pulverizadores'),
    ('Implementos/Plantadeiras', 'Disco de Distribuição'),
    ('Implementos/Plantadeiras', 'Disco para Plantadeiras'),
    ('Implementos/Plantadeiras', 'Gerais para Implementos/Plantadeiras'),
    ('Rolamentos', 'Rolamentos')
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


def getAplicacao(soup, categoria):
    aplicacao = ''
    produto = ''
    try:
        teste = soup.findAll('div', class_='formulario60')[0]
        for li in teste.find_all("p"): 
            if li.text.strip().find(categoria.strip()) > -1:
                aplicacao  = li.text.strip()
    except:
        aplicacao = '-'
    try:
        teste = soup.findAll('tab', class_='tabprodutos')[0]
        for li in teste.find_all("td"): 
            if li.text.strip().find("CÓDIGO") > -1:
                produto  = li.next_sibling.text.strip()
    except:
        produto = '-'
    return aplicacao, produto

def get_data(soup, categoria, webD, oldUrl):
    product_list = []

    for item in soup.findAll('div', class_='produtox'):
        
        dat_fato = current_date
        des_produto = item.findAll('div', class_='produto2')[0].text.strip() 
        des_categoria = categoria[0] + '-' +  categoria[1]
        
        try:
            price = float(item.findAll('span', class_='sp22')[0].text.strip().replace(",","."))
        except:
            price = 0.0
        try:
            installment = item.findAll('span', class_='sp4')[0].text.strip().replace("ou ", "")
        except:
            installment = ''

        source = 'SiteX'
        marca = '-'
        aplicacao = '-'
        link = item.find_all("a")[0]
        new_url = link.get("href")
        webD.get(new_url)
        aplicacao, cod_produto = getAplicacao(BeautifulSoup(webD.page_source, "html.parser"),  des_produto[0:des_produto.find(' - ',0,len(des_produto))])
        webD.get(oldUrl)
        product_list.append((dat_fato, des_produto, cod_produto, des_categoria, marca, price, installment, source, aplicacao))

    df = pd.DataFrame(data=product_list, columns=column_data_types.keys())
    return df


def scrap_SiteX(categorias: list):
    
    final_df = None
    url = f'https://www.SiteX.com.br/'    
    new_url = url
    browser = webdriver.Chrome(options=options)
    browser.get(url)
    sleep(5)
    cont = 1
    for categoria in categorias:
        
        print(cont ,' de ' , len(categorias))
        cont = cont + 1
        page_content = browser.page_source
        soup = BeautifulSoup(page_content, "html.parser")
        links = soup.find_all("a") # Find all elements with the tag <a>
        for link in links:
            if  link.string is not None and link.string.strip() == categoria[1].strip():
                #print("Link:", link.get("href"), "Text:", link.string)
                new_url = link.get("href")
                break
        browser.get(new_url)
        sleep(5)
        df = get_data(soup=soup, categoria=categoria, webD = browser, oldUrl = url)
        if final_df is None:
            final_df = df
        elif df is not None:
            final_df = pd.concat([final_df, df])
        
    return final_df

# Executa webscraping
df = scrap_SiteX(categorias)
if not df.empty:
    df['VAL_PRECO'] = df['VAL_PRECO'].astype(float)
    df['DAT_FATO'] = pd.to_datetime(df['DAT_FATO']).dt.strftime('%Y-%m-%d')
    df.to_excel('D:/SiteXscrapping.xlsx', index = False)

print('Processo finalizado...')