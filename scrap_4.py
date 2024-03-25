from bs4 import BeautifulSoup
import re
import os
from dotenv import load_dotenv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
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

def getAplicacao(soup):
    aplicacao = ''
    marca = ''
    try:
        teste = soup.findAll('div', class_='tab-pane active')[0]
        for li in teste.find_all("li"): 
            aplicacao  = aplicacao + '/' + li.text.strip()
    except:
        aplicacao = '-'

    try:
        teste = soup.findAll('div', class_='tab-pane active')[0]
        for textos in teste.find_all("span", style=lambda value: value and 'font-weight: 700' in value): 
            if textos.text.strip().find("Marca") > -1:
                marca  = textos.next_sibling.text.strip()
    except:
        teste = '-'

    return aplicacao, marca

def get_data(soup, webD, oldUrl):
    product_list = []

    for item in soup.findAll('div', class_='caption'):

        dat_fato = current_date
        des_produto = item.findAll('h4', class_='product-name')[0].text.strip()
        cod_produto = des_produto.split('-')[-1]
        des_categoria = 'Colheitadeira'

        if item.findAll('span', class_='price-new') != []:
            price = get_standard_number(item.findAll('span', class_='price-new')[0].text)
        else:
            price = get_standard_number(item.findAll('div', class_='price')[0].text)

        installment = item.findAll('div', class_='parcelamento')[0].text.strip()
        source = 'SiteX parts'
        link = item.find_all("a")[0]
        new_url = link.get("href")
        webD.get(new_url)
        aplicacao, marca = getAplicacao(BeautifulSoup(webD.page_source, "html.parser"))
        webD.get(oldUrl)
        product_list.append((dat_fato, des_produto, cod_produto, des_categoria, marca, price, installment, source, aplicacao))

    df = pd.DataFrame(data=product_list, columns=column_data_types)
    return df


def scrap_SiteX_parts():
    
    final_df = None
    keep_scrapping = True
    page = 1

    while keep_scrapping == True:

        try:
            url = f'https://www.SiteXparts.com.br/index.php?route=product/category&path=140_146_156&page={page}'
            browser = webdriver.Chrome(options=options)
            browser.get(url)
            sleep(5)

            page_content = browser.page_source
            soup = BeautifulSoup(page_content, "html.parser")
            df = get_data(soup=soup, webD = browser, oldUrl = url)

            if df.shape[0] != 0:
                if final_df is None:
                    final_df = df
                else:
                    final_df = pd.concat([final_df, df])
                
                page += 1
                browser.quit()
            else:
                browser.quit()
                keep_scrapping = False
                
        except:
            browser.quit()
            keep_scrapping = False
    
    return final_df

# Executa webscraping
df = scrap_SiteX_parts()

if not df.empty:
    df['VAL_PRECO'] = df['VAL_PRECO'].astype(float)
    df['DAT_FATO'] = pd.to_datetime(df['DAT_FATO']).dt.strftime('%Y-%m-%d')

    # Grava arquivo no lake
    df.to_excel('D:/Webscrapping/SiteXparts.xlsx', index = False)

print('Processo finalizado...')
