from ast import Bytes
from webbrowser import get
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging 
from .constants import SERVER_NAME
from .ext_days import day_escriturador , day_str
import json
from io import BytesIO
from datetime import datetime


class Maps():


    def __init__(self,login,password):
        self.login = login
        self.password = password

    def formlogin(self):
        return {
            'username': self.login,
            'password': self.password
        }

    def maps_login(self):
        with requests.Session() as s:
            # Finding the authentication needed to gain access to Pegasus Module
            url = 'https://ot.cloud.mapsfinancial.com/auth/realms/mapscloud/protocol/openid-connect/auth?client_id=mapspegasuspassivo&redirect_uri=https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/home&response_type=code&scope=openid%20offline_access&state=y8CrrwZXZttzwBQA'
            r = s.get(url)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=self.formlogin())
        return s

class MapsEscriturador(Maps):

    def login_escriturador(self):
        with requests.Session() as s:
            url = "https://ot.cloud.mapsfinancial.com/escriturador/login" 
            r = s.get(url)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=self.formlogin())            
        return s

    def ativos_escriturador(self):
        with self.login_escriturador() as s:
            ativos_escriturador = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/ativos/relatorio"
            r = s.get(ativos_escriturador)    
            open(f"Consulta Ativos.xlsx",'wb').write(r.content)
            #add_to_base(pd_table)
            logging.info(f'Extração Ativos escriturador,realizada com sucesso')
            time.sleep(1)
            s.close()

    def movimentos(self,ativoid,datainicial):
        '''data no formato  yyyy-mm-dd'''   
        with self.login_escriturador() as s:
            movimentos = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes?identificadorInvestidor=&dataInicio=2022-01-01&dataFim=2022-01-31&depositaria=1&filtrarTipo=true&pagina"
            movimentos2 = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes?ativoId={ativoid}&identificadorInvestidor=&dataInicio={datainicial}&dataFim={datainicial}&depositaria=3&filtrarTipo=true&pagina"
            r = s.get(movimentos2)   
            # df = pd.DataFrame.from_dict(json.loads(r.content))
            return json.loads(r.content)


    def movimentos_escriturais(self, datainicial): 
        '''data no formato  yyyy-mm-dd'''   
        with self.login_escriturador() as s:
            try:
                movimentos = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes?identificadorInvestidor=&dataInicio=2022-01-01&dataFim=2022-01-31&depositaria=1&filtrarTipo=true&pagina"
                movimentos2 = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes?identificadorInvestidor=&dataInicio={datainicial}&dataFim={datainicial}&depositaria=3&filtrarTipo=true&pagina"
                r = s.get(movimentos2)   
                # df = pd.DataFrame.from_dict(json.loads(r.content))
                retorno = json.loads(r.content)
                if len(retorno)  == 0:
                    return [{"resultado": "Sem Movimentos"}]
                else:
                    return retorno
            except Exception as e:
                print (e)
                return [{"resultado": "Sem Movimentos"}]

    
    def consulta_eventos(self,data,depositaria):
        '''data no formato  yyyy-mm-dd'''
        try:
            with self.login_escriturador() as s:
                relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/eventosEfetivados/findEventosEfetivadosDinheiro?dataLiquidacao={data}&depositaria={depositaria}&pagina"
                relatorio2 = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/eventosEfetivados/countEventosEfetivadosDinheiro?dataLiquidacao={data}&depositaria={depositaria}"
                r = s.get(relatorio2)
                print (r.content)
                if int(r.content.decode('utf-8'))> 0:
                    r = s.get(relatorio)       
                    base = json.loads(r.content.decode('utf-8'))
                    return  base
                elif int(r.content.decode('utf-8')) == 0:
                    return [{'resultado':"sem eventos"  ,"depositaria": depositaria}]
        except Exception as e:
            print(e)
            print ("Sem Evento para o dia requisitado")
            return [{'resultado':"erro"}]


    def ativos_df(self):
        try:
            with self.login_escriturador() as s:
                relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/ativos?pagina"
                
                r = s.get(relatorio)
                print (r)
                base = json.loads(r.content.decode('utf-8'))['elementos']
                resultado = pd.DataFrame.from_dict(base)
                return  resultado
        except Exception as e:
            print ("erro na requisição")
            print (e)

        
class MapsPegasus(Maps):



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

    #definir se a extração vai ser por dia ou por periodo

    def patrimonio_cota_classe(self):
        try:
            with self.maps_login() as s:
                cota_classe = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/bookmarkable/web.pages.consulta.carteira.patrimonioClasse.ConsultaPatrimonioCotaClasse?1'
                r = s.get(cota_classe, headers=headers)
                # Making the post_request with my params
                pendencia = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/page?1-1.IFormSubmitListener-mainForm'
                r = s.post(pendencia, data=self.create_form(i,day_str()), headers=headers)
                soup2 = BeautifulSoup(r.content,'html.parser')
                table = soup2.find('table')            
                pd_table = pd.read_html(str(table),thousands=".",decimal=",")[0]                
                print(f'Extração carteira {i} feita com sucesso')
                time.sleep(1)
                s.close()
        except Exception as e:
                print (e)
    

    def extrair_rentabilidade_fundos(self,dt_i):
        f = {
            'idd_hf_0': "",
            'entity:entityInfo': '{"id":100000,"naturalKey":"MAPS","persistableClass":"jmine.biz.invest.base.carteira.domain.GrupoCarteira"}',
            'entity:autoComplete': 'MAPS',
            'dataInicio:control-group:control-group_body:_input': dt_i,
            'dataFim:control-group:control-group_body:_input': dt_i,
            'pageCommands:elements:0:cell': 'Pesquisar'
        }

        with requests.Session() as s:
            # Finding the authentication needed to gain access to Pegasus Module
            url = 'https://ot.cloud.mapsfinancial.com/pegasus/main'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=self.formlogin(), headers=headers)
            lista_pendencia = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/bookmarkable/web.pages.consulta.carteira.rentabilidadePeriodo.ConsultaRentabilidadeCarteiraPeriodo?'
            r = s.get(lista_pendencia, headers=headers)
            # Making the post_request with my params
            pendencia = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/page?1-1.IFormSubmitListener-mainForm'
            r = s.post(pendencia, data=f, headers=headers)
            soup2 = BeautifulSoup(r.content, 'html.parser')
            table = soup2.find('table')
            pd_table = pd.read_html(str(table), thousands=".", decimal=",")[0]
            s.close()
            return pd_table[['Data', 'Carteira', 'CNPJ', 'Tipo Carteira', 'Cota', 'PL']]


    def extrair_cotas_pegasus(self, cotas):
        #incluir a extração de cotas do pegasus
        pass




class MapsCentaurus(Maps):

    def ajustar_df(self , df):
        investidor = ""
        ndf = []
        for item in df.iterrows():
            if 'Investidor' in str(item[1]['Nº Operação']):
                investidor = item[1]['Nº Operação']
            base_dict = item[1].to_dict()
            base_dict['investidor'] = investidor.replace("Investidor: ", "")
            ndf.append(base_dict)
        novodf = pd.DataFrame.from_dict(ndf)
        return novodf[(novodf['Status'].notna()) & (novodf['Status'] != "Status") ]

    def form_centaurus_unique(self,papel,dt_i,dt_f):
        form_centaurus = {
                "id1f_hf_0": "",
                "papelCota:control-group:control-group_body:_input": papel,
                "dtInicio:control-group:control-group_body:_input": dt_i,
                "dtFim:control-group:control-group_body:_input": dt_f,
                "pageCommands:elements:0:cell": "Pesquisar" }
        return form_centaurus

    def form_centaurus(self,data):
        form_centaurus = {
                "id1f_hf_0": "",
                "papelCota:control-group:control-group_body:_input": "",
                "dtInicio:control-group:control-group_body:_input": data,
                "dtFim:control-group:control-group_body:_input": data,
                "pageCommands:elements:0:cell": "Pesquisar" }
        return form_centaurus
    
    def extrair_movimentacoes_investidor(self,papelcota,investidor,dt_i,dt_f):
        '''datas no formato dt_i,dt_f'''
        url_mov = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.movimentacao.cota.investidor.ConsultaMovimentacaoCotaInvestidor?1"
        url_resp = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm"
        form = {
            "id1e_hf_0": "",
            "investidor:control-group:control-group_body:_input": investidor,
            "papelCota:control-group:control-group_body:_input": papelcota,
            "tipoDataPesquisa": "0",
            "dataInicio:control-group:control-group_body:_input": dt_i,
            "dataFim:control-group:control-group_body:_input": dt_f,
            "tiposMovimento": "",
            "pageCommands:elements:0:cell": "Pesquisar"
        }
        with self.maps_login() as s:
            r = s.get(url_mov)
            r = s.post(url_resp,form)
            soup = BeautifulSoup(r.content,'html.parser')
            csv_get = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?2-IResourceListener-report-report_exporter-elements-2-cell"
            r = s.get(csv_get)            
            df = pd.read_csv(BytesIO(r.content),delimiter=";")
            print (df[[ "Nº Operação", "Tipo Operação","Data Operação",
                        "Data Processamento","Data Conversão","Data Liquidação",
                        "Data do Fundo na Movimentação","Data Referência",
                        "Data Cancelamento","Quantidade","Valor Bruto","Valor IOF",
                        "Valor IR","Valor Impostos Compensados","Taxa Operacional","Valor Líquido",
                        "Status"]].iloc[1: , :])

    def extrair_movimentacoes_fundo(self,papelcota,dt_i):
        form = {
            "ida_hf_0": "" , 
            "papelCota:control-group:control-group_body:_input": papelcota, 
            "tipoDataPesquisa": "0", 
            "dataInicio:control-group:control-group_body:_input": dt_i, 
            "dataFim:control-group:control-group_body:_input": dt_i, 
            "tiposMovimento": "",
            "distribuidor": "",
            "pageCommands:elements:0:cell": "Pesquisar", 
        }
        urltela = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.movimentacao.cota.fundo.ConsultaMovimentacaoCotaFundo?1"
        ext_form =   "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm"
        excel_get = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?2-IResourceListener-report-report_exporter-elements-1-cell"
        with self.maps_login() as s:
            r = s.get(urltela)            
            r = s.post(ext_form,form)
            r = s.get(excel_get)       
            df1 = pd.read_excel(BytesIO(r.content),skiprows=4 , engine='openpyxl')
            df2 = pd.read_excel(BytesIO(r.content),skiprows=2 , engine='openpyxl')
            df2.columns = df1.columns
            df2['data'] =  dt_i
            if df2.empty:
                return [{"resultado":  "Sem Movimentos"}]
            else:            
                df2['mnemonico'] = papelcota
                return self.ajustar_df(df2)
      
    def posicao_movimentacoes(self,data,papel_cota):
        form =  {
                "d2d_hf_0": "",
                "papelCota:control-group:control-group_body:_input": papel_cota,
                "dataRef:control-group:control-group_body:_input": data,
                "distribuidor": "",
                "grupoFundo": "",
                "pageCommands:elements:2:cell": "Download CSV"
            }
        url_posicao = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.posicao.cota.fundo.ConsultaPosicaoCotaFundo?1"
        url_resp_form = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm"
        with self.maps_login() as s:
            r = s.get(url_posicao)
            r = s.post(url_resp_form,form)
            try:
                df = pd.read_csv(BytesIO(r.content),delimiter=";" ,  thousands='.' ,  decimal=',')
                df.rename(columns={"Papel Cota": "papelcota" ,  'Data Referência': 'dataposicao'})
 
                return df[df['N° Operação'] != 'Total:']
            except Exception as e:
                return "na"

    def get_investidor(self):
        with self.maps_login() as s:
            url_1 = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.cadastro.investidor.PesquisaInvestidor?1"
            csv = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-IResourceListener-mainForm-pesquisa-report_exporter-elements-3-cell"
            url_2 = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-2.IFormSubmitListener-mainForm"
            url_3 = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.cadastro.investidor.PesquisaInvestidor?2"
            r = s.get(url_1)
            r = s.get(csv)

            df = pd.read_csv(BytesIO(r.content),delimiter=";")
            df['cpf_cnpj_jcot'] = df['CNPJ/CPF'].apply(lambda x : x.replace(".","").replace("/","").replace("-",""))
        
            return df

      
    def eventosefetivados(self,data,papel_cota):
        form =  {
            'idc_hf_0':  ""  , 
            "investidor:control-group:control-group_body:_input": ""  , 
            "papelCota:control-group:control-group_body:_input": papel_cota , 
            "dataLiquidacao:control-group:control-group_body:_input": data, 
            "contaCorrente:control-group:control-group_body:_input": ""  , 
            "digito:control-group:control-group_body:_input": "" , 
            "tipoEventoCota:control-group:control-group_body:_input":""  , 
            "cnpj:control-group:control-group_body:_input": "" , 
            "pageCommands:elements:0:cell": "Pesquisar" , 
            }
        url_posicao = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.evento.cota.ConsultaEventoEfetivadoCota?1"
        url_resp_form = "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm"
        get_csv =  "https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?2-IResourceListener-report-report_exporter-elements-2-cell"
        with self.maps_login() as s:
            r = s.get(url_posicao)
            r = s.post(url_resp_form,form)
            r =  s.get(get_csv)
            df = pd.read_csv(BytesIO(r.content),delimiter=";" , encoding="utf-8")
            return df
            # try:
            #     df = pd.read_csv(BytesIO(r.content),delimiter=";")
            #     df.rename(columns={"Papel Cota": "papelcota"}) 
            #     return df[df['Tipo Pessoa'].notna()]
            # except Exception as e:
            #     return "na"


    def get_posicao_consolidada(self,papelcota,df):
        try:
            with requests.Session() as s:
                url = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main'
                r = s.get(url)
                soup = BeautifulSoup(r.content, 'html.parser')
                x = soup.find('form')['action']
                r = s.post(x, data = self.formlogin())
                pos_centaurus = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.fundo.posicao_consolidado.ConsultaPosicaoConsolidado?1'
                r = s.get(pos_centaurus)
                cent_bram = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm='
                r = s.post(cent_bram, data = self.form_centaurus_unique(papelcota,df,df))
                table_soup = BeautifulSoup(r.content, 'html.parser')
                r = s.get("https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?2-IResourceListener-report-report_exporter-elements-2-cell")
                dataframe = pd.read_csv(BytesIO(r.content),delimiter=";")
                return dataframe.to_dict('records')[0]
        except:
            return {
                "Quantidade": 0 , 
                "Principal": 0 , 
                "Saldo Bruto":  0
            }
            

            


































def add_to_base(df):
    
    base = {}
    base['Carteira'] = []
    base['Cota'] = []
    base['Data']  = []
    base['Valor da Cota'] = []
    base['Patrimônio Líquido'] = []
    base['Valor das Emissões'] = []
    base['Qtde. de Cotas das Emissões'] = []
    base['Valor dos Resgates'] = []
    base['Qtde. de Cotas dos Resgates'] = []
    base['Qtde. de Cotas Total'] = []   
    for row in df.iterrows():
        base['Carteira'].append(row[1]['Carteira'])
        base['Cota'].append(row[1]['Cota']) 
        base['Data'].append(row[1]['Data'])  
        base['Valor da Cota'].append(row[1]['Valor da Cota']) 
        base['Patrimônio Líquido'].append(row[1]['Patrimônio Líquido']) 
        base['Valor das Emissões'].append(row[1]['Valor das Emissões']) 
        base['Qtde. de Cotas das Emissões'].append(row[1]['Qtde. de Cotas das Emissões']) 
        base['Valor dos Resgates'].append(row[1]['Valor dos Resgates']) 
        base['Qtde. de Cotas dos Resgates'].append(row[1]['Qtde. de Cotas dos Resgates']) 
        base['Qtde. de Cotas Total'].append(row[1]['Qtde. de Cotas Total']) 
    return base


def movimentacao():
    with requests.Session() as s:
        #login escriturador
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        r = s.post(x, data=form, headers=headers)
        print(f'Você está logado...')    
        movimentacao = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes/relatorio?identificadorInvestidor=&dataInicio=2021-08-04&dataFim=2021-08-04&filtrarTipo=false"
        r = s.get(movimentacao, headers=headers)
        open(f"tempfiles/movimentacao.xlsx",'wb').write(r.content)
        logging.info(f'Extração Ativos escriturador,realizada com sucesso')
        time.sleep(1)
        s.close()








def form_liberacao_cota():
    param_pendencia = {
    'idb_hf_0': '',
    'carteira:control-group:control-group_body:_input': '',
    'grupoCarteira:control-group:control-group_body:_input': '',
    'data:control-group:control-group_body:_input': day_str(),
    'pageCommands:elements:0:cell': 'Pesquisar'
    }
    return param_pendencia

def liberacao_cota():
    with requests.Session() as s:
        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/pegasus/main'
        r = s.get(url, headers=headers)
        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        # Making my posting requesting
        r = s.post(x, data=form, headers=headers)
        print(f'Você está logado...')
        # Get my "pendencia_screen"   
        lista_pendencia = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/bookmarkable/web.pages.cota.liberar.LiberarCotaPage'

        r = s.get(lista_pendencia, headers=headers)
        # Making the post_request with my params
        pendencia = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/page?1-1.IFormSubmitListener-mainForm'
        r = s.post(pendencia, data=form_liberacao_cota(), headers=headers)
        soup2 = BeautifulSoup(r.content,'html.parser')
        table = soup2.find('table')
        #print (table)    
        #pd_table = pd.read_html(str(table),thousands=".",decimal=",")[0]

        excel_get = 'https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/page?2-IResourceListener-mainForm-pesquisa-report_exporter-elements-1-cell'
        r = s.get(excel_get, headers=headers)

        open(f"batimentos-temp/Pesquisa Liberação Cota.xlsx",'wb').write(r.content)

        #add_to_base(pd_table)
        logging.info(f'Status de Cota extraido com sucesso')
        time.sleep(1)
        s.close()


def identificar_pegasus():
        with requests.Session() as s:
        # Finding the authentication needed to gain access to Pegasus Module
            url = 'https://ot.cloud.mapsfinancial.com/pegasus/main'
            r = s.get(url, headers=headers)
            # Extracting the complete url with all the parameter
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            # Making my posting requesting
            r = s.post(x, data=form, headers=headers)
            print(f'Você está logado...')

            tipo_cota_carteira = "https://ot.cloud.mapsfinancial.com/pegasus/main/wicket/bookmarkable/jmine.tec.web.wicket.pages.main.ExternalPage?4&IFRAME_SOURCE=/pegasus/protected/cota/list.faces%3FSessionCleanerFilter.CLEAN%3D1%26BreadCrumb%3D:Base:Carteira:Tipos+de+Cotas+da+Carteira"

            r = s.get(tipo_cota_carteira)

            soup_tip = BeautifulSoup(r.content , 'html5lib')

            print (soup_tip.prettify())
            
            s.close()





def form_escriturador():
    return  day_escriturador()

def quantidades_escriturador():
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
        # Get my "pendencia_screen"   
        lista_pendencia = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/relatorioQuantidadeIntegralizada/exportQuantidadeTotalIntegralizada/?data={form_escriturador()}&apresentaCessionario=false&agrupador=1"

        r = s.get(lista_pendencia, headers=headers)

        open(f"batimentos-temp/Quantidade integralizada.xlsx",'wb').write(r.content)

   
        print(f'Extração Quantidade Integralizada , realizada com sucesso')
        time.sleep(1)
        s.close()




def consulta_investidor(cnpj):
    with requests.Session() as s:
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        r = s.post(x, data=form, headers=headers)
        invest = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/investidores?nome=&cpfCnpj={cnpj}&numeroConta=&pagina"
        r = s.get(invest)
        base = json.loads(r.content.decode('utf-8'))['elementos'][0]
        resultado  = {
            'id': [base['id']] ,
            'nome': [base['nome']] ,
            'cpfCnpj': [base['cpfCnpj']] ,
            'naturezaInvestidorEnum': [base['naturezaInvestidorEnum'] ],
            'descricaoNaturezaInvestidor': [base['descricaoNaturezaInvestidor']] ,
            'paisResidencia':  [base['paisResidencia']['nome']] ,
            'cpfCnpjSemMascara': [base['cpfCnpjSemMascara']]
        }
        df = pd.DataFrame.from_dict(resultado)
        return df





def consulta_investidor_id(cnpj,dt):
    with requests.Session() as s:
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        r = s.post(x, data=form, headers=headers)
        invest = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/investidores?nome=&cpfCnpj={cnpj}&numeroConta=&pagina=1"
        r = s.get(invest)
        id_invest = json.loads(r.content.decode("utf-8"))['elementos'][0]['id']                
        pos_investidor  = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/relatorioPosicaoInvestidor/arquivo/?data={dt}&investidor={id_invest}&apresentaCessionario=false&agrupador=1" 
        r = s.get(pos_investidor)
        df = pd.read_excel(BytesIO(r.content),skiprows=1)
        s.close()
        df['CPF/CNPJ Investidor'] = df['CPF/CNPJ Investidor'].apply(lambda x : x.replace(".","").replace("-","").replace("/",""))
        return df[['Ativo','Investidor','CPF/CNPJ Investidor','Quantidade total','Depositária']]
 
        
def consulta_posicao_investidor(id_investidor , data_posicao):
    with requests.Session() as s:
        url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
        r = s.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html5lib')
        x = soup.find('form')['action']
        r = s.post(x, data=form, headers=headers)
        pos_investidor  = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/relatorioPosicaoInvestidor/arquivo/?data={data_posicao}&investidor={id_investidor}&apresentaCessionario=false&agrupador=1" 
        r = s.get(pos_investidor)
        #result = open('result.xlsx','wb').write(r.content)
        df = pd.read_excel(BytesIO(r.content),skiprows=1)
        s.close()
        return df[['Ativo','Investidor','Quantidade total' , 'Depositária']]


def consulta_carteira_completa(cpfcnpj, dataposicao):
        id_investidor = consulta_investidor_id(cpfcnpj)
        with requests.Session() as s:
            url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=form, headers=headers)
            pos_investidor  = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/relatorioPosicaoInvestidor/arquivo/?data={dataposicao}&investidor={id_investidor}&apresentaCessionario=false&agrupador=1" 
            r = s.get(pos_investidor)
            #result = open('result.xlsx','wb').write(r.content)
            df = pd.read_excel(BytesIO(r.content),skiprows=1)
            s.close()
        return df[['Ativo','Investidor','Quantidade total' , 'Depositária']]





def get_posicao_consolidada_unique():
    with requests.Session() as s:
        # Finding the authentication needed to gain access to Pegasus Module
        url = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main'
        r = s.get(url, headers = headers)
        # Extracting the complete url with all the parameter
        soup = BeautifulSoup(r.content, 'html.parser')
        x = soup.find('form')['action']
        # Making my posting requesting
        r = s.post(x, data = form, headers = headers)
        pos_centaurus = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/bookmarkable/centaurus.web.pages.consulta.fundo.posicao_consolidado.ConsultaPosicaoConsolidado?1'
        r = s.get(pos_centaurus, headers = headers)
        cent_bram = 'https://ot.cloud.mapsfinancial.com/pegasuspassivo/main/wicket/page?1-1.IFormSubmitListener-mainForm='
        r = s.post(cent_bram, data = form_centaurus_unique("190901 FIM","30/07/2021"), headers = headers)
        table_soup = BeautifulSoup(r.content, 'html.parser')
        table = table_soup.find('table')
        pd_table = pd.read_html(str(table),thousands='.',decimal=',')[0]
        pd_table.to_excel(f"tempfiles/posicao_consolidada.xlsx",index=False)
        logging.info(f'Extração Posição Consolidada ,realizada com sucesso')
        s.close()

def get_investidores():
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
        # Get my "pendencia_screen"   
        lista_pendencia = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/investidores/relatorio?nome=&cpfCnpj=&numeroConta="
        r = s.get(lista_pendencia, headers=headers)
        open(f"investidores.xlsx",'wb').write(r.content)
        logging.info(f'Extração Quantidade Integralizada , realizada com sucesso')
        time.sleep(1)
        s.close()
  

def ajustar_data(data):
    dfdata = datetime.strptime(data, '%Y-%m-%d')
    data_ajustada = dfdata.strftime("%d/%m/%Y")
    return data_ajustada


def movimentacoes_ativo(codigo):
    try:
        with requests.Session() as s:
            url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            consulta = f"https:i//ot.cloud.mapsfinancial.com/escriturador/rest/ativos?codigoAtivo={codigo}&pagina=1"
            r = s.post(x, data=form, headers=headers)
            r = s.get(consulta,headers=headers)
            id = json.loads(r.content.decode('utf-8'))['elementos'][0]['id']
            dtini = '2021-08-01'
            dtfim = '2021-08-31'
            relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/acoes/relatorio?ativoId={id}&identificadorInvestidor=&dataInicio={dtini}&dataFim={dtfim}&filtrarTipo=false"
            #relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes/relatorio?ativoId={id}&identificadorInvestidor=&filtrarTipo=false"
            r = s.get(relatorio,headers=headers)
            open(f'folder_cetip_termp/{codigo}.xlsx','wb').write(r.content)
    except:
        pass
        # df = pd.read_excel(BytesIO(r.content))
        # df.to_excel(f"{codigo}.xlsx")
        # df['Data movimentação'] = df['Data movimentação'].dt.strftime("%d/%m/%Y")
        # #df['CPF/CNPJ - cedente'] = df['CPF/CNPJ - cedente'].apply(lambda x : x.replace(".","").replace("/","").replace("-",""))
        # df.to_csv(f"movimentacao-{codigo}.csv",index=False)


def movimentacoes_escriturais(dtini):
    try:
        with requests.Session() as s:
            url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=form, headers=headers)
            dtfim = dtini
            relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/movimentacoes?identificadorInvestidor=&dataInicio={dtini}&dataFim={dtfim}&depositaria=3&filtrarTipo=true&pagina"
            r = s.get(relatorio,headers=headers)
            return  json.loads(r.content.decode('utf-8'))
    except:
        print ("erro na requisição")
        pass



def ativos_detalhe(id):
    try:
        with requests.Session() as s:
            url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=form, headers=headers)
            relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/ativos/{id}"
            r = s.get(relatorio,headers=headers)    
            base = json.loads(r.content.decode('utf-8'))
            
            resultado = pd.DataFrame.from_dict(base)
            return  resultado
    except:
        print ("erro na requisição")
        pass



#consultaeventosefetivados em dinheiro





        
def consulta_eventos_t():
    try:
        with requests.Session() as s:
            # url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            # r = s.get(url, headers=headers)
            # soup = BeautifulSoup(r.content, 'html5lib')
            # x = soup.find('form')['action']
            # r = s.post(x, data=form, headers=headers)
            login_header = {
                'MAPS_CLIENTE_ID':'thiago.conceicao' ,
                'MAPS_CLIENT_SECRET': 'tAman2001**'
            }
            relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/eventosEfetivados/findEventosEfetivadosDinheiro?dataLiquidacao=2021-09-29&pagina"
            r = s.get(relatorio,headers=login_header)
            return  r.content
    except Exception as e:
        print (e)
        print ("erro na requisição")
        



def invests_depositaria():
    try:
        with requests.Session() as s:
            url = 'https://ot.cloud.mapsfinancial.com/escriturador/app'
            r = s.get(url, headers=headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            x = soup.find('form')['action']
            r = s.post(x, data=form, headers=headers)
            relatorio = f"https://ot.cloud.mapsfinancial.com/escriturador/rest/ativos?pagina"
            r = s.get(relatorio,headers=headers)
            base = json.loads(r.content.decode('utf-8'))['elementos']
            resultado = pd.DataFrame.from_dict(base)
            return  resultado
    except:
        print ("erro na requisição")
        pass

