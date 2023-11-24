from MAPS_MODULE.extracoes import MapsCentaurus
from JCOTSERVICE import RelPosicaoFundoCotistaService
from conciliar import  get_investidores
from JCOTSERVICE import MovimentoResumidoService
import pandas as pd
from datetime import date , datetime  , timedelta
import os 
from pymongo import MongoClient
import time


client  = MongoClient("mongodb://localhost:27017")


def get_cota_planilha(path , data):
    df = pd.read_excel(path ,  sheet_name="pesquisa" , skiprows=1)
    print (data)
    try:
        resultado = df[df['Data']== data].to_dict('records')[0]
        return resultado
    except Exception as e:
        print (e)
        return { 
            'Valor da Cota': 0
        }



class MovimentosTransfer():    

    def __init__(self ,  cd_jcot,  fundo_maps , data_inicial ,  data_final):
        self.fundo_jcot = cd_jcot 
        self.fundo_maps = fundo_maps
        self.centaurus = MapsCentaurus("thiago.conceicao" ,  "tAman1996**")
        self.jcot_posicao = RelPosicaoFundoCotistaService("roboescritura" ,  "Senh@123")
        self.movimentos_service = MovimentoResumidoService("roboescritura" ,  "Senh@123")
        self.data_inicial = data_inicial 
        self.data_final =  data_final
       # self.data_inicial = datetime(2023,7 , 31)  # inicio_importacao
       # self.data_final =  datetime(2023,9 ,12 )  # fim importacao

    def tipo_movimentacao(self , tipo_movimentacao):
        depara = {
            "APLICAÇÃO": 	'A' , 
            "RESGATE TOTAL":	'RT' , 
            "RESGATE PARCIAL":	'RB' }
        try:
            return depara[tipo_movimentacao]
        except:
            return "revisar tipo"

    def get_itens_a_lancar(self ,  dia):
        movimentos = self.centaurus.extrair_movimentacoes_fundo(self.fundo_maps, dia.strftime("%d/%m/%Y"))
        movimentos['cotista'] =  movimentos['investidor'].apply(get_investidores)
        movimentos['tipo'] = movimentos['Tipo Operação'].apply(self.tipo_movimentacao)
        movimentos['valor'] =  movimentos['Valor Bruto'].apply(abs)
        movimentos['qtdcotas'] = movimentos['Quantidade'].apply(abs)
        movimentos['clearing'] = 'STR'
        movimentos['liquidacao'] = 'LI'
        movimentos['fundo'] =  self.fundo_jcot
        movimentos['data'] = movimentos['data'].apply(lambda x: datetime.strptime( x ,"%d/%m/%Y").strftime("%Y-%m-%d"))

        movimentos.to_excel("3105.xlsx")

        movimentos_agrupados =  movimentos.groupby(['cotista' , 'tipo' , 'clearing' , 
                                                    'fundo'  , "liquidacao",  'data'  ]).sum(['valor' ,  'qtdcotas']).reset_index()
        
        return movimentos_agrupados.to_dict("records")

    def importar_movimentos_jcot(self ,  dia):
        lancamentos = []
        itens_a_lancar =  self.get_itens_a_lancar(dia)
        for movimento in itens_a_lancar:
            if movimento['tipo'] != "revisar tipo":
                try:
                    print (movimento)
                    teste =  self.movimentos_service.movimentoResumidoRequest(dados=movimento)
                    lancamentos.append(teste)
                    print(teste)
                except Exception as e:
                    print (e)
                    continue
            
        client['log_lancamentos'][self.fundo_jcot].insert_many(lancamentos)
     
    def processar_dia_fundo(self ,  dia):
        '''dia deve ser uma instância de datetime'''
        
        job  =  {"codigo": self.fundo_jcot ,  "data_processamento":  dia.strftime("%Y-%m-%d"),  
                 "dataPosicao": dia.strftime("%Y-%m-%d") , "cota_processamento" :  self.get_cota_dia(dia), 
                  "tipo": "abertura" ,"status": "FECHADO" }

        string_cli = f"-codigo {job['codigo']} -dataProcessamento {job['data_processamento']} -cota_processamento {job['cota_processamento']} -dataPosicao {job['dataPosicao']} -tipo {job['tipo']} -status {job['status']}"
        print (string_cli)
        os.system(f"processamentoautomatico.exe {string_cli} ")
        time.sleep(2)

    def get_cota_dia(self, dia ):
        cota = self.centaurus.get_posicao_consolidada(self.fundo_maps , dia.strftime("%d/%m/%Y") )
        print (cota)
        # cota = get_cota_planilha('patrimonio_classe_XP 051 FIAGRO III_XP 051 FIAGRO III_2023-01-01_2023-09-26.xlsx' , dia.strftime("%d/%m/%Y"))
        try:
            return cota['Valor de Cota']
        except:
            return "0"
       
    def processarPeriodo(self , datainicioprocessamento):
        '''data inicio processamento precisa ser um datetime '''
        while datainicioprocessamento != self.data_final:
                self.processar_dia_fundo(datainicioprocessamento)
                datainicioprocessamento = datainicioprocessamento + timedelta(days=1)

    def atualizar_fundo(self):
        while self.data_inicial != self.data_final:
            try:
                print (self.data_inicial.strftime("%d/%m/%Y"))
                self.importar_movimentos_jcot(self.data_inicial)
                
            except Exception as e:
                print(e)
            finally:
                # self.processar_dia_fundo(self.data_inicial)
                self.data_inicial = self.data_inicial + timedelta(days=1)
                time.sleep(2)





