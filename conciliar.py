from MAPS_MODULE.extracoes import MapsCentaurus
from JCOTSERVICE import RelPosicaoFundoCotistaService
from sqlalchemy import create_engine , text
import pandas as pd 
from datetime import date , datetime


engine = create_engine("sqlite:///conciliador.db")


class ConciliacaoMapsJcot():


    centaurus = MapsCentaurus("thiago.conceicao" ,  "tAman1996**")
    jcot_posicao = RelPosicaoFundoCotistaService("roboescritura" ,  "Senh@123")

    
    data = datetime(2023,6,14)

    def __init__(self ,  codigo_jcot , codigo_maps , data):
        '''a data precisa ser um datetiime'''
        self.cd_maps = codigo_maps
        self.cd_jcot = codigo_jcot
        self.fundo = {
            "codigo_jcot":codigo_jcot , 
            "nome": codigo_maps
        }
        self.data = data
        
    #atualizar_investidores


    def atualizar_investidores(self):

        con = engine.connect()
        con.execute(text("drop table if exists investidores"))
        con.commit()
        con.close()
        investidores = self.centaurus.get_investidor()
        investidores.to_sql("investidores", con=engine , if_exists='append')


    def get_investidores(self , string_nome_investidor):
        if "XP INV" not in string_nome_investidor:
            sql = f"select * from investidores where Investidor =  '{string_nome_investidor}' "
            consulta = pd.read_sql(sql, con=engine)
            try:
                return consulta['cpf_cnpj_jcot'].values[0]
            except:
                return "não encontrado"
        else:
            base = string_nome_investidor.split("Distribuidor")[0]
            return base.replace("XP INVESTIMENTOS" , "XP").strip()


    def atualizar_conciliacao(self , data , fundo):

        con = engine.connect()
        con.execute(text("drop table if exists posicoes_jcot"))
        con.execute(text("drop table if exists posicoes_maps"))
        con.commit()
        con.close()

        df =  self.jcot_posicao.get_posicoes_table({"codigo": fundo['codigo_jcot'] ,  "dataPosicao": data.strftime("%Y-%m-%d")})
        df['cd_cotista'] = df['cd_cotista'].apply(lambda x: x.strip())
        df.to_sql("posicoes_jcot" ,  con=engine  , if_exists='append')

        df = self.centaurus.posicao_movimentacoes(data.strftime("%d/%m/%Y") ,  fundo['nome'])
        df['cd_jcot'] = df['Investidor'].apply(self.get_investidores)
        
        df.to_sql("posicoes_maps", con=engine , if_exists='append')


    def gerar_conciliacao(self, data):

        sql_maps = f'''SELECT DISTINCT  posicoes_maps.cd_jcot from posicoes_maps
    where posicoes_maps."Data Referência" = "{data.strftime("%d/%m/%Y")}"
    group by posicoes_maps.cd_jcot'''
        
        sql_jcot = f'''select DISTINCT posicoes_jcot.cd_cotista from posicoes_jcot
    where posicoes_jcot.dataPosicao  =  "{data.strftime("%Y-%m-%d")}"
    group by   posicoes_jcot.cd_cotista '''
        

        sql_relatorio = f'''
    with 
        posicoes_maps_resumido as (
        select posicoes_maps.cd_jcot , 
        sum(posicoes_maps."Qtde. Disponivel")  + sum(posicoes_maps."Qtde. Bloq.") as qtd , 
        "maps"as tipo
        from posicoes_maps 
        group by posicoes_maps.cd_jcot
        ) , posicoes_jcot_resumido as (
        select posicoes_jcot.cd_cotista , 
        sum(posicoes_jcot.qtCotas) as qtd , "jcot" as tipo from posicoes_jcot 
        group by posicoes_jcot.cd_cotista 
        ) , consolidado as (
            select * from posicoes_maps_resumido
        union all
        select * from posicoes_jcot_resumido
        )
        
        select cd_jcot , SUM(CASE WHEN tipo='jcot' THEN qtd END) as qtdJcot ,
        SUM(CASE WHEN tipo='maps' THEN qtd END) as qtdMaps   , 
        SUM(CASE WHEN tipo='jcot' THEN qtd END)  - SUM(CASE WHEN tipo='maps' THEN qtd END) 	as diferenca
        from consolidado
        group by cd_jcot
        order by diferenca desc
        '''
        
        investidores_maps = pd.read_sql(sql_maps, con=engine)
        investidores_jcot = pd.read_sql(sql_jcot , con=engine)
        df = pd.read_sql(sql_relatorio , con=engine)
        df['data_conciliacao'] = self.data.strftime("%d/%m/%Y")

        return df

    def conciliar(self):

        self.atualizar_conciliacao(self.data, self.fundo)
        self.gerar_conciliacao(self.data).to_excel(f'conciliacoes/conciliacoes_{self.data.strftime("%d%m%Y")}.xlsx')






conciliador = ConciliacaoMapsJcot("30991" , 
                                  "JIVE BOSSANOVA HIGH YIELD ADVISORY FIC FIM CP" , 
                                  datetime(2023 , 6 , 13))


conciliador.conciliar()