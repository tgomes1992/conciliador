from .util import *
import pandas as pd

#notas de aplicacao/entrada

class PosicaoNotaHistorica():

    VALORPADRAO = 0
    LISTA =   []

    def __init__(self,idnota,dt_posicao,cd_cotista,
                    cd_fundo,dt_aplicacao, vlr_aplicacao,
                    vlr_cota_aplicacao,vlr_cota_ultimo,qtcotas,
                    vlr_liquido,vlr_bruto,vlr_ir):
        self.idnota = idnota
        self.dt_posicao = dt_posicao
        self.cd_cotista = cd_cotista
        self.cd_fundo = cd_fundo
        self.dt_aplicacao = dt_aplicacao
        self.vlr_aplicacao  = vlr_aplicacao
        self.vlr_cota_aplicacao = vlr_cota_aplicacao
        self.vlr_cota_ultimo = vlr_cota_ultimo
        self.vlr_ufir_aplicacao = 0
        self.qtd_cotas = qtcotas
        self.vlr_corrigido = strtofloat(vlr_bruto)
        self.vlr_bruto = strtofloat(vlr_bruto)
        self.cd_liquidacao =  "DI"
        self.cd_clearing = "STR"
        self.ic_sn_liberada = "S"
        self.ic_sn_colagem = "N"
        self.vlr_ir = vlr_ir
        self.vlr_liquido = vlr_liquido

    def get_base_dict(self):  
        base_dict = {
            "ID_NOTA":self.idnota,
            "DT_POSICAO":self.dt_posicao,
            "CD_COTISTA":self.cd_cotista,
            "CD_FUNDO":self.cd_fundo,
            "IC_SN_LIBERADA": self.ic_sn_liberada,
            "DT_APLICACAO":self.dt_aplicacao,
            "VL_APLICACAO":self.vlr_aplicacao,
            "VL_COTA_APLICACAO":self.vlr_cota_aplicacao,
            "VL_UFIR_APLICACAO":self.VALORPADRAO ,
            "DT_ULTIMO_ANIVER_CORRIDO":self.dt_posicao,
            "DT_ULTIMO_ANIVER_UTIL":self.dt_posicao,
            "VL_COTA_ULTIMO_ANIVER_UTIL":self.vlr_cota_ultimo,
            #"DT_PROXIMO_ANIVER_UTIL":self.dt_posicao , # calcular o dia útil seguinte
            #"DT_PROXIMO_ANIVER_CORRIDO":self.dt_posicao, # calcular o dia útil seguinte
            "DT_PROXIMO_ANIVER_UTIL":"03/01/2022" , # calcular o dia útil seguinte
            "DT_PROXIMO_ANIVER_CORRIDO":"03/01/2022",
            "QT_COTAS":self.qtd_cotas,
            "VL_CORRIGIDO": self.vlr_corrigido,
            "VL_BRUTO": self.vlr_bruto,
            "VL_CORRIGIDO_PERFORMANCE":self.VALORPADRAO,
            "VL_IR": self.vlr_ir,
            "VL_IOF":self.VALORPADRAO,
            "VL_PIP":self.VALORPADRAO,
            "VL_IR_PIP":self.VALORPADRAO,
            "VL_PENALTY_FEE": self.VALORPADRAO,
            "VL_PERFORMANCE":self.VALORPADRAO,
            "VL_RENDIMENTO_COMPENSAR":self.VALORPADRAO,
            "VL_TAXA_ADMINISTRACAO":self.VALORPADRAO,
            "VL_DEVOLUCAO_TAXA_ADMIN": self.VALORPADRAO,
            "VL_DEVOLUCAO_PERFORMANCE":self.VALORPADRAO,
            "VL_RECEITA_SAQUE_CARENCIA":self.VALORPADRAO,
            "VL_RESGATE":self.vlr_liquido,
            "DT_ULTIMO_RESGATE_IR":self.dt_aplicacao, 
            "VL_COTA_ULTIMO_RESGATE_IR":self.vlr_cota_aplicacao,
            "DT_PROXIMO_RESGATE_IR":"31/05/2022", #data do come cotas
            "DT_INICIO_PERFORMANCE":"",
            "VL_COTA_PERFORMANCE":self.VALORPADRAO,
            "VL_IOF_VIRTUAL":"",
            "VL_RENDIMENTO_RESGATE_IR":"",
            "VL_CUSTO_MEDIO":"",
            "VL_IOF_APLICACAO":"",
            "ID_NOTA_ORIGEM":"",
            "DT_TRANSFERENCIA":"",
            "VL_PERFORMANCE_ATIVO":"",
            "VL_COTA_301294":"",
            "DT_ULTIMO_IR_LIMINAR":"",
            "VL_PERFORMANCE_ORIGINAL":"",
            "VL_COTA_ULTIMO_IR_LIMINAR":"",
            "VL_COTA_311201":"",
            "VL_CUSTO_MEDIO_311201":"",
            "VL_VARIACAO_PERFORMANCE":"",
            "IC_SN_PFEE_NOTA":"",
            "IC_SN_RESGATE_PFEE":"",
            "VL_REND_1":"",
            "PC_ALIQ_1":"",
            "VL_REND_2":"",
            "PC_ALIQ_2":"",
            "VL_COTA_311204":"",
            "VL_REND_ULTIMO_COME_COTAS":"",
            "VL_REND_PRIM_COME_COTAS":"",
            "VL_REND_SEGU_COME_COTAS":"",
            "VL_REND_TERC_COME_COTAS":"",
            "VL_DESENQ_RENDIMENTO":"",
            "VL_DESENQ_RENDIMENTO_COMPL":"",
            "CD_DESENQ":"",
            "VL_DESENQ_RENDIMENTO_1":"",
            "VL_DESENQ_RENDIMENTO_COMPL_1":"",
            "CD_DESENQ_1":"",
            "PC_DESENQ_ALIQ_IR":"",
            "PC_DESENQ_ALIQ_IR_1":"",
            "PC_DESENQ_ALIQ_IR_COMPL":"",
            "PC_DESENQ_ALIQ_IR_COMPL_1":"",
            "VL_IR_CONTA_ORDEM":"",
            "VL_IOF_CONTA_ORDEM":"",
            "CD_LIQUIDACAO":"",
            "CD_CLEARING":"",
            "ID_BANCO":"",
            "CD_AGENCIA":"",
            "CD_CONTA":"",
            "PC_ALIQUOTA_PERFORMANCE":"",
            "PC_ALIQUOTA_IR":"",
            "VL_CUSTO_CONTABIL":"",
            "VL_FATOR_TX_PRE_1":"",
            "VL_FATOR_TX_PRE_2":"",
            "VL_FATOR_INDEX_1":"",
            "VL_FATOR_INDEX_2":"",
            "QT_DIAS_IOF":"",
            "VL_ALIQUOTA_IOF":"",
            "IC_PV_TIPO_IOF":"",
            "IC_SN_COLAGEM":self.ic_sn_colagem,
            "VL_AGIO_DESAGIO":"",
            "VL_COTA_SALDO_INFORME": self.vlr_cota_aplicacao,
            "IC_SN_LIBERADA_RESGATE":self.ic_sn_liberada,
            "VL_TAXA_GESTAO":""
            }
       
        base_dict["VL_IOF_VIRTUAL"] = self.VALORPADRAO
        base_dict["VL_RENDIMENTO_RESGATE_IR"] = self.VALORPADRAO
        base_dict["VL_CUSTO_MEDIO"] = self.VALORPADRAO
        base_dict["VL_IOF_APLICACAO"] = self.VALORPADRAO      
        base_dict["VL_PERFORMANCE_ATIVO"] = self.VALORPADRAO
        base_dict["VL_COTA_301294"] = self.VALORPADRAO
        base_dict["VL_PERFORMANCE_ORIGINAL"] = self.VALORPADRAO
        base_dict["VL_COTA_ULTIMO_IR_LIMINAR"] = self.VALORPADRAO
        base_dict["VL_COTA_311201"] = self.VALORPADRAO
        base_dict["VL_CUSTO_MEDIO_311201"] = self.VALORPADRAO
        base_dict["VL_VARIACAO_PERFORMANCE"] = self.VALORPADRAO        
        base_dict["VL_REND_1"] = self.VALORPADRAO
        base_dict["PC_ALIQ_1"] = self.VALORPADRAO
        base_dict["VL_REND_2"] = self.VALORPADRAO
        base_dict["PC_ALIQ_2"] = self.VALORPADRAO
        base_dict["VL_COTA_311204"] = self.VALORPADRAO
        base_dict["VL_REND_ULTIMO_COME_COTAS"] = self.VALORPADRAO
        base_dict["VL_REND_PRIM_COME_COTAS"] = self.VALORPADRAO
        base_dict["VL_REND_SEGU_COME_COTAS"] = self.VALORPADRAO
        base_dict["VL_REND_TERC_COME_COTAS"] = self.VALORPADRAO
        base_dict["VL_DESENQ_RENDIMENTO"] = self.VALORPADRAO
        base_dict["VL_DESENQ_RENDIMENTO_COMPL"] = self.VALORPADRAO
        base_dict["VL_DESENQ_RENDIMENTO_1"] = self.VALORPADRAO
        base_dict["VL_DESENQ_RENDIMENTO_COMPL_1"] = self.VALORPADRAO
        base_dict["PC_DESENQ_ALIQ_IR"] = self.VALORPADRAO
        base_dict["PC_DESENQ_ALIQ_IR_1"] = self.VALORPADRAO
        base_dict["PC_DESENQ_ALIQ_IR_COMPL"] = self.VALORPADRAO
        base_dict["PC_DESENQ_ALIQ_IR_COMPL_1"] = self.VALORPADRAO
        base_dict["VL_IR_CONTA_ORDEM"] = self.VALORPADRAO
        base_dict["VL_IOF_CONTA_ORDEM"] = self.VALORPADRAO
        base_dict["CD_LIQUIDACAO"] = self.cd_liquidacao
        base_dict["CD_CLEARING"] = self.cd_clearing
        base_dict["PC_ALIQUOTA_PERFORMANCE"] = self.VALORPADRAO
        base_dict["PC_ALIQUOTA_IR"] = self.VALORPADRAO
        base_dict["VL_CUSTO_CONTABIL"] = self.VALORPADRAO
        base_dict["VL_FATOR_TX_PRE_1"] = self.VALORPADRAO
        base_dict["VL_FATOR_TX_PRE_2"] = self.VALORPADRAO
        base_dict["VL_FATOR_INDEX_1"] = self.VALORPADRAO
        base_dict["VL_FATOR_INDEX_2"] = self.VALORPADRAO
        base_dict["QT_DIAS_IOF"] = self.VALORPADRAO
        base_dict["VL_ALIQUOTA_IOF"] = self.VALORPADRAO
        base_dict["IC_PV_TIPO_IOF"] = self.VALORPADRAO
        base_dict["VL_AGIO_DESAGIO"] = self.VALORPADRAO
        base_dict["VL_TAXA_GESTAO"] = self.VALORPADRAO
        return base_dict
       
