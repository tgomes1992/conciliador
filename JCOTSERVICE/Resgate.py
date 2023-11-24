from .util import *
from datetime import datetime


class Resgate():
 

   def __init__(self,idnota ,idnotaaplicacao,dt_resgate,dt_nota_aplicacao,qt_cotas,vlr_cota_aplicacao,
                vlr_aplicacao,vlr_corrigido,vlr_iof,vlr_bruto_resgate,vlr_ir , vlr_liquido_resgate, 
                vlr_rendimento_tributado,vlr_rendimento_isento,vlr_cota_utilizada, vl_custo_medio):
       self.idnota = idnota
       self.idnotaaplicaao = idnotaaplicacao
       self.dt_resgate = dt_resgate  #data do movimento
       self.dt_aplicacao = dt_nota_aplicacao
       self.qtd_cotas = qt_cotas
       self.vlr_cota_aplicacao  =   vlr_cota_aplicacao
       self.vlr_aplicacao = vlr_aplicacao
       self.vlr_corrigido = vlr_corrigido
       self.vlr_iof = vlr_iof
       self.vlr_brutoresgate = vlr_bruto_resgate
       self.vlr_ir = vlr_ir
       self.vlr_liquido_resgate = vlr_liquido_resgate
       self.vlr_rendimento_tributado = vlr_rendimento_tributado
       self.vlr_rendimento_isento = vlr_rendimento_isento
    #    self.dt_cota_utilizada =  dt_cota_aplicacao
       self.vlr_cota_utilizada = vlr_cota_utilizada
       self.vl_custo_medio = vl_custo_medio
       self.VL_REND_ULTIMO_COME_COTAS = 0	
       self.VL_REND_PRIM_COME_COTAS	 = 0
       self.VL_REND_SEGU_COME_COTAS = 0
       self.VL_REND_TERC_COME_COTAS = 0
       self.vl_corrigido_performance = 0


   def get_base_dict(self):
        base_dict = {
            "ID_NOTA":self.idnota,
            "ID_NOTA_APLICACAO":self.idnotaaplicaao,
            "DT_RESGATE":datetime.strptime(str(self.dt_resgate),"%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y"),
            "DT_APLICACAO":self.dt_aplicacao,
            "QT_COTAS": abs(strtofloat(self.qtd_cotas)),
            "VL_COTA_APLICACAO":self.vlr_cota_aplicacao,
            "VL_APLICACAO":self.vlr_aplicacao,
            "VL_CORRIGIDO":  abs(strtofloat(self.vlr_brutoresgate)),
            "VL_RECEITA_SAQUE_CARENCIA":"",
            "VL_IOF":self.vlr_iof,
            "VL_BRUTO_RESGATE":abs(strtofloat(self.vlr_brutoresgate)),
            "VL_IR":abs(self.vlr_ir),
            "PC_ALIQUOTA_IR":"",
            "VL_PENALTY_FEE":0,
            "VL_RENDIMENTO_COMPENSADO":0,
            "VL_LIQUIDO_RESGATE": abs(strtofloat(self.vlr_liquido_resgate)),
            "VL_PERFORMANCE":0,
            "VL_PIP":0,
            "VL_IR_PIP":0,
            "VL_TAXA_ADMINISTRACAO":0,
            "VL_DEVOLUCAO_TAXA_ADMIN":0,
            "VL_DEVOLUCAO_PERFORMANCE":0,
            "VL_RENDIMENTO_TRIBUTADO":0,
            "VL_RENDIMENTO_ISENTO":0,
            "DT_COTA_UTILIZADA":datetime.strptime(str(self.dt_resgate),"%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y"),
            "VL_COTA_UTILIZADA":self.vlr_cota_utilizada,
            "VL_IOF_VIRTUAL":0,
            "VL_RENDIMENTO_RESGATE_IR":0,
            "VL_CUSTO_MEDIO":0,
            "VL_IOF_APLICACAO":0,
            "VL_PERFORMANCE_ATIVO":0,
            "PC_ALIQ_1":0,
            "VL_REND_1":0,
            "PC_ALIQ_2":0,
            "VL_REND_2":0,
            "VL_REND_1_COMPENSADO":0,
            "VL_REND_2_COMPENSADO":0,
            "VL_REND_ULTIMO_COME_COTAS":0,
            "VL_REND_PRIM_COME_COTAS":0,
            "VL_REND_SEGU_COME_COTAS":0,
            "VL_REND_TERC_COME_COTAS":0,
            "VL_DESENQ_RENDIMENTO":0,
            "VL_DESENQ_RENDIMENTO_COMPL":0,
            "CD_DESENQ":"",
            "VL_DESENQ_RENDIMENTO_1":0,
            "VL_DESENQ_RENDIMENTO_COMPL_1":0,
            "CD_DESENQ_1":"",
            "VL_IR_CONTA_ORDEM":0,
            "VL_IOF_CONTA_ORDEM":0,
            "VL_CUSTO_CONTABIL":0,
            "VL_AGIO_DESAGIO":0,
            "QT_DIAS_IOF":"",
            "VL_ALIQUOTA_IOF":0,
            "IC_PV_TIPO_IOF": "",
            "VL_TAXA_GESTAO":0,
            "VL_IR_MERC_SEC":0,
            "VL_IOF_MERC_SEC":0,
            "VL_REND_TRIBUTADO_MERC_SEC":0,
            "VL_REND_ISENTO_MERC_SEC":0,
            "VL_IR_COMPENSADO_DISTR_REND":0,
            "VL_CORRIGIDO_PERFORMANCE":abs(self.vl_corrigido_performance),
        }
        return base_dict



   def get_base_dict2(self):
        base_dict = {
            "ID_NOTA":self.idnota,
            "ID_NOTA_APLICACAO":self.idnotaaplicaao,
            "DT_RESGATE":self.dt_resgate,
            "DT_APLICACAO":self.dt_aplicacao,
            "QT_COTAS": self.qtd_cotas,
            "VL_COTA_APLICACAO":self.vlr_cota_aplicacao,
            "VL_APLICACAO":self.vlr_aplicacao,
            "VL_CORRIGIDO":  self.vlr_brutoresgate,
            "VL_RECEITA_SAQUE_CARENCIA":"",
            "VL_IOF":self.vlr_iof,
            "VL_BRUTO_RESGATE":self.vlr_brutoresgate,
            "VL_IR":abs(self.vlr_ir),
            "PC_ALIQUOTA_IR":"",
            "VL_PENALTY_FEE":0,
            "VL_RENDIMENTO_COMPENSADO":0,
            "VL_LIQUIDO_RESGATE": self.vlr_liquido_resgate,
            "VL_PERFORMANCE":0,
            "VL_PIP":0,
            "VL_IR_PIP":0,
            "VL_TAXA_ADMINISTRACAO":0,
            "VL_DEVOLUCAO_TAXA_ADMIN":0,
            "VL_DEVOLUCAO_PERFORMANCE":0,
            "VL_RENDIMENTO_TRIBUTADO":0,
            "VL_RENDIMENTO_ISENTO":0,
            "DT_COTA_UTILIZADA":self.dt_resgate,
            "VL_COTA_UTILIZADA":self.vlr_cota_utilizada,
            "VL_IOF_VIRTUAL":0,
            "VL_RENDIMENTO_RESGATE_IR":0,
            "VL_CUSTO_MEDIO":0,
            "VL_IOF_APLICACAO":0,
            "VL_PERFORMANCE_ATIVO":0,
            "PC_ALIQ_1":0,
            "VL_REND_1":0,
            "PC_ALIQ_2":0,
            "VL_REND_2":0,
            "VL_REND_1_COMPENSADO":0,
            "VL_REND_2_COMPENSADO":0,
            "VL_REND_ULTIMO_COME_COTAS":0,
            "VL_REND_PRIM_COME_COTAS":0,
            "VL_REND_SEGU_COME_COTAS":0,
            "VL_REND_TERC_COME_COTAS":0,
            "VL_DESENQ_RENDIMENTO":0,
            "VL_DESENQ_RENDIMENTO_COMPL":0,
            "CD_DESENQ":"",
            "VL_DESENQ_RENDIMENTO_1":0,
            "VL_DESENQ_RENDIMENTO_COMPL_1":0,
            "CD_DESENQ_1":"",
            "VL_IR_CONTA_ORDEM":0,
            "VL_IOF_CONTA_ORDEM":0,
            "VL_CUSTO_CONTABIL":0,
            "VL_AGIO_DESAGIO":0,
            "QT_DIAS_IOF":"",
            "VL_ALIQUOTA_IOF":0,
            "IC_PV_TIPO_IOF": "",
            "VL_TAXA_GESTAO":0,
            "VL_IR_MERC_SEC":0,
            "VL_IOF_MERC_SEC":0,
            "VL_REND_TRIBUTADO_MERC_SEC":0,
            "VL_REND_ISENTO_MERC_SEC":0,
            "VL_IR_COMPENSADO_DISTR_REND":0,
            "VL_CORRIGIDO_PERFORMANCE":abs(self.vl_corrigido_performance),
        }
        return base_dict













    





