'''
RESUMO SCRIPT: Script para realizar a extração das bases de cadastro utilizadas nos robôs da área de escrituração
AREA: SQESCRITURAÇÃO
GESTOR : JOÃO BEZERRA
FEITO POR:  THIAGO GOMES
ENVIADO PARA TI EM AGOSTO /2021
'''

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging 






# form = {
#     'username':  get_login()['user'],
#     'password':  get_login()['senha']
# }


headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0'}


def create_form(carteira, data):
    form = {
    'carteira': carteira,
    'grupoCarteira': "",    
    'cota' : "",
    'containerDataInicio:dataInicio': data,
    'containerDataFim:dataFim': data, 
    'pageCommands:elements:0:cell' : "Pesquisar"
    }
    return form   

def ativos_escriturador():
    with requests.Session() as s:
        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        # Making my posting requesting
        r = s.post(x, data=form, headers=headers)


        print(f'Você está logado...')

        down_ativos = f"https://ot.cloud.mapsfinancial.com/escriturador/consultaAtivos"

        r = s.get(down_ativos, headers=headers)
     
        ativos_escriturador = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/ativos/relatorio"

        r = s.get(ativos_escriturador, headers=headers)

        #print (table)    
        #pd_table = pd.read_html(str(table),thousands=".",decimal=",")[0]

       # r = s.get(excel_get, headers=headers)

        open(f"tempfiles/Consulta Ativos.xlsx",'wb').write(r.content)

        #add_to_base(pd_table)
        logging.info(f'Extração Ativos escriturador,realizada com sucesso')
        time.sleep(1)
        s.close()

def emissores_escriturador():
    with requests.Session() as s:
        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        # Making my posting requesting
        r = s.post(x, data=form, headers=headers)


        print(f'Você está logado...')

        down_ativos = f"https://ot.cloud.mapsfinancial.com/escriturador/consultaEmissores"

        r = s.get(down_ativos, headers=headers)
     
        ativos_escriturador = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/emissores/relatorio?nome=&cnpj="

        r = s.get(ativos_escriturador, headers=headers)

        #print (table)    
        #pd_table = pd.read_html(str(table),thousands=".",decimal=",")[0]

       # r = s.get(excel_get, headers=headers)

        open(f"tempfiles/Emissores.xlsx",'wb').write(r.content)

        #add_to_base(pd_table)
        logging.info(f'Extração Ativos escriturador,realizada com sucesso')
        time.sleep(1)
        s.close()

def form_centaurus(data):
    form_centaurus = {
            "id1f_hf_0": "",
            "papelCota:control-group:control-group_body:_input": "",
            "dtInicio:control-group:control-group_body:_input": data,
            "dtFim:control-group:control-group_body:_input": data,
            "pageCommands:elements:0:cell": "Pesquisar" }
    return form_centaurus

def cadastro_fundos():
    with requests.Session() as s:

        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main'
        r = s.get(url, headers = headers)

        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']

        # Making my posting requesting
        r = s.post(x, data = form, headers = headers)

        pos_centaurus = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.cadastro.fundo.PesquisaFundo?1'
      
        r = s.get(pos_centaurus, headers = headers)

        relatorio_fundos = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-IResourceListener-mainForm-pesquisa-report_exporter-elements-1-cell'
        r =  s.get(relatorio_fundos, headers=headers)

        open(f"tempfiles/fundos_centaurus.xlsx",'wb').write(r.content)

        logging.info(f'Extração Fundos Concluída')
        
        s.close()

def emissao_papel_cota():
    with requests.Session() as s:

        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main'
        r = s.get(url, headers = headers)

        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']

        # Making my posting requesting
        r = s.post(x, data = form, headers = headers)

        pos_centaurus = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.cadastro.emissaocota.PesquisaEmissaoCota?1'
      
        r = s.get(pos_centaurus, headers = headers)

        relatorio_fundos = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-IResourceListener-mainForm-pesquisa-report_exporter-elements-1-cell'
        r =  s.get(relatorio_fundos, headers=headers)

        open(f"tempfiles/Fundo-Valor_Cota.xlsx",'wb').write(r.content)

        logging.info(f'Extração de Fundos Papel Cota Concluída')
        
        s.close()



def cadastros():
    ativos_escriturador()
    emissores_escriturador()
    cadastro_fundos()
    emissao_papel_cota()


