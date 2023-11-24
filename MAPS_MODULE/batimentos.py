from .extracoes import *
import pandas as pd
from .constants import DATA_PATH ,BATIMENTOS_FOLDER , criar_pastas


def leitura_patrimonio_cota(cota_pegasus):
    file = pd.read_excel("batimentos-temp/Patrimonio Cota Clase.xlsx")
    filter = file[file['Cota'] == cota_pegasus]
    try:
        dados = {
            'qtd': filter['Qtde. de Cotas Total'].values[0] ,
            'status': 'ok'
        }
    except:
        dados = {
            'qtd': 0 ,
            'status': 'não localizado'
        }

    return dados

def leitura_escriturador(cota_escriturador):
    file = pd.read_excel("batimentos-temp/Quantidade integralizada.xlsx",skiprows=1)
    filter = file[file['Ativo'] == cota_escriturador]
    try:
        dados = {
            'qtd': filter['Quantidade total integralizada'].values[0] ,
            'status': 'ok'
        }
    except:
        dados = {
            'qtd': 0 ,
            'status': 'não localizado'
        }

    return dados

def identificadores(cota):
    file = pd.read_excel(f"{DATA_PATH}/papel_cota.xlsx")
    filter = file[file.papel_cota==cota]
    try:
        dados = {
            'ativo' : filter['identificador_ativo'].values[0] ,
            'escriturador':  filter['identificador__escriturador'].values[0] ,
        }
    except:
        dados = {
            'ativo' : 'na' ,
            'escriturador':  'na',
        }
    return dados
    
def leitura_posicao_consolidada(cota):
    file = pd.read_excel("batimentos-temp/posicao_consolidada.xlsx")
    filtro = file[file.Mnemonico==cota]
    try:
        to_dict = filtro.to_dict('records')[0]
        dados = {
            'cota':  to_dict['Valor de Cota'] , 
            'quantidade': to_dict['Quantidade']
        }
    except:
        dados = {
            'cota':  0 , 
            'quantidade':0
        }
    return dados

def leitura_liberacao_cota(cota_pegasus):
    file = pd.read_excel("batimentos-temp/Pesquisa Liberação Cota.xlsx",skiprows=2)
    filter = file[file['Nome']==cota_pegasus]
    try:
        dados = {
            'status':  filter['Liberação'].values[0], 
            'cota' :  filter['Bruto'].values[0]
        }
    except:
        dados = {
            'status':  'na', 
            'cota' :  0
        }
    return dados

def leitura_quantidade_integralizada():
    pass

def extracoes_batimentos(): 
    get_posicao_consolidada()
    quantidades_escriturador()
    liberacao_cota()
    patrimonio_cota_classe()

def batimento_cota():
    posicao_consolidada = pd.read_excel("batimentos-temp/posicao_consolidada.xlsx")
    base = {
        'papel_centaurus': [] ,
        'cota_centaurus' : [],
        'papel_pegasus':  [] ,
        'cota_pegasus': [] ,
        'diferença': [] ,    
        'status': []
        }
    for row in posicao_consolidada.iterrows():
        base['papel_centaurus'].append(row[1].Mnemonico)
        base['cota_centaurus'].append(row[1]['Valor de Cota'])
        cota_pegasus = identificadores(row[1].Mnemonico)['ativo']
        print (cota_pegasus)
        diferenca =row[1]['Valor de Cota'] - leitura_liberacao_cota(cota_pegasus)['cota']
        base['papel_pegasus'].append(cota_pegasus)
        base['cota_pegasus'].append(leitura_liberacao_cota(cota_pegasus)['cota'])
        base['diferença'].append(diferenca)
        base['status'].append(leitura_liberacao_cota(cota_pegasus)['status'])
    df = pd.DataFrame.from_dict(base)
    df.to_excel(f'{BATIMENTOS_FOLDER}/batimento_cota.xlsx',index=False)

def get_cotas_fora():
    lista = pd.read_excel(f"batimentos-temp/cotas_fora_batimentos.xlsx")['nao_bater'].values
    return lista



def batimento_quantidade():
    posicao_consolidada = pd.read_excel("batimentos-temp/posicao_consolidada.xlsx")
    base = {
        'papel_centaurus': [] ,
        'qtd_centaurus' : [],
        'qtd_pegasus': [] ,
        'qtd_escriturador': [] ,
        'diferença_centaurus-pegasus': [] ,
        'diferenca centaurus-escriturador': []
         }
    for row in posicao_consolidada.iterrows():
        if row[1].Mnemonico not in get_cotas_fora():
            base['papel_centaurus'].append(row[1].Mnemonico)
            base['qtd_centaurus'].append(row[1].Quantidade)
            cota_pegasus = identificadores(row[1].Mnemonico)['ativo']
            qtd_pegasus = leitura_patrimonio_cota(cota_pegasus)['qtd']
            base['qtd_pegasus'].append(qtd_pegasus)
            diferenca = row[1].Quantidade - qtd_pegasus
            base['diferença_centaurus-pegasus'].append(diferenca)
            cota_escriturador = identificadores(row[1].Mnemonico)['escriturador']
            qtd_escriturador = leitura_escriturador(cota_escriturador)['qtd']
            dif2 = row[1].Quantidade - qtd_escriturador
            base['qtd_escriturador'].append(qtd_escriturador)
            base['diferenca centaurus-escriturador'].append(dif2)
    df = pd.DataFrame.from_dict(base)
    df.to_excel(f'{BATIMENTOS_FOLDER}/batimento-Quantidade.xlsx',index=False)



def batimentos():
    extracoes_batimentos()
    criar_pastas()
    batimento_cota()
    batimento_quantidade()





