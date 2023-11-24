from MAPS_MODULE.extracoes import MapsCentaurus
from JCOTSERVICE import RelPosicaoFundoCotistaService
from sqlalchemy import create_engine , text
import pandas as pd 
from datetime import date , datetime


engine = create_engine("sqlite:///conciliador.db")


centaurus = MapsCentaurus("thiago.conceicao" ,  "tAman1932**")
jcot_posicao = RelPosicaoFundoCotistaService("roboescritura" ,  "Senh@123")


#atualizar_investidores


# con = engine.connect()
# con.execute(text("delete from investidores"))
# con.commit()
# con.close()
# investidores = centaurus.get_investidor()
# investidores.to_sql("investidores", con=engine , if_exists='append')


def get_investidores(string_nome_investidor):
    if "XP INV" not in string_nome_investidor:
        sql = f"select * from investidores where Investidor =  '{string_nome_investidor}' "
        consulta = pd.read_sql(sql, con=engine)
        try:
            return consulta['cpf_cnpj_jcot'].values[0]
        except:
            return "não encontrado"
    else:
        base = string_nome_investidor.split("Distribuidor")[0]
        return base.replace("XP INVESTIMENTOS" , "XP")


# buscar_posicoes_jcot







# # # # #buscar_posicoes_maps


def atualizar_conciliacao(data , fundo):

    con = engine.connect()
    con.execute(text("drop table if exists posicoes_jcot"))
    con.execute(text("drop table if exists posicoes_maps"))
    con.commit()
    con.close()

    df =  jcot_posicao.get_posicoes_table({"codigo": fundo['codigo_jcot'] ,  "dataPosicao": data.strftime("%Y-%m-%d")})
    df['cd_cotista'] = df['cd_cotista'].apply(lambda x: x.strip())
    print (df.head())
    df.to_excel("posicoes_jcot.xlsx")
    df.to_sql("posicoes_jcot" ,  con=engine  , if_exists='append')

    df = centaurus.posicao_movimentacoes(data.strftime("%d/%m/%Y") ,  fundo['nome'])
    df['cd_jcot'] = df['Investidor'].apply(get_investidores)
    print (df.head())
    df.to_excel("posicoes_maps.xlsx")
    
    df.to_sql("posicoes_maps", con=engine , if_exists='append')


def gerar_conciliacao(data):

    sql_maps = f'''SELECT DISTINCT  posicoes_maps.cd_jcot from posicoes_maps
where posicoes_maps."Data Referência" = "{data.strftime("%d/%m/%Y")}"
group by posicoes_maps.cd_jcot'''
    
    sql_jcot = f'''select DISTINCT posicoes_jcot.cd_cotista from posicoes_jcot
where posicoes_jcot.dataPosicao  =  "{data.strftime("%Y-%m-%d")}"
group by   posicoes_jcot.cd_cotista '''
    

    sql_relatorio = f'''select posicoes_maps.cd_jcot  ,posicoes_jcot.cd_cotista ,  posicoes_maps."Data Referência"  , 
Investidor , sum(posicoes_maps."Qtde. Disponivel")  + sum(posicoes_maps."Qtde. Bloq.") as QtdTotalMaps ,
sum( DISTINCT posicoes_jcot.qtCotas) as qtdJcot ,  
sum(posicoes_maps."Qtde. Disponivel")  + sum(posicoes_maps."Qtde. Bloq.") - sum( DISTINCT posicoes_jcot.qtCotas) as diferenca
from posicoes_maps 
inner join posicoes_jcot
on posicoes_jcot.cd_cotista  =  posicoes_maps.cd_jcot 
where posicoes_maps."Data Referência" = "{data.strftime("%d/%m/%Y")}" and posicoes_jcot.dataPosicao  = "{data.strftime("%Y-%m-%d")}"
group by posicoes_maps.cd_jcot ,  posicoes_jcot.cd_cotista 
having diferenca  > 0.1'''
    
    investidores_maps = pd.read_sql(sql_maps, con=engine)

    investidores_jcot = pd.read_sql(sql_jcot , con=engine)

    investidores_analisar = [investidor for investidor in investidores_maps['cd_jcot'].values if investidor 
                             not in investidores_jcot.cd_cotista.values ]
    
    print ("fora da jcot mas está no maps")
    print (investidores_analisar)
    
    investidores_analisar2 = [investidor for investidor in investidores_jcot.cd_cotista.values if investidor 
                            not in investidores_maps['cd_jcot'].values  ]
    print ("fora da maps mas está no jcot")
    print (investidores_analisar2)

    df = pd.read_sql(sql_relatorio , con=engine)

    return df


