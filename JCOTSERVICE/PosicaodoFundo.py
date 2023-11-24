import pandas as pd
from .util import *


class PosicaodoFundo():


    VALOR_PADRAO = 0

    def  __init__(self,codigofundo,dataposicao,afposicao,vl_patrimonio_abertura,
        vlr_cota_abertura,vlr_patrimonio_fechamento, qtd_cotas_fechamento,vlr_cota_fechamento):
        self.cdfundo = codigofundo
        self.dtposicao = dataposicao
        self.afposicao = afposicao
        self.vlr_patrimonio_abertura = strtofloat(vl_patrimonio_abertura)
        self.vlr_cota_abertura = strtofloat(vlr_cota_abertura)
        self.vlr_patrimonio_fechamento = strtofloat(vlr_patrimonio_fechamento)
        self.qtd_cotas_fechamento = strtofloat(qtd_cotas_fechamento)
        self.vlr_cota_fechamento  = strtofloat(vlr_cota_fechamento)
    
    def get_base_dict(self):
        BASE_DICT = {
    "CD_FUNDO": "",
    "DT_POSICAO":"",
    "IC_AF_POSICAO":"",
    "VL_PATRIMONIO_ABERTURA":"",
    "VL_COTA_ABERTURA":"",
    "VL_COTA_PRE_PFEE":"",
    "VL_PATRIMONIO_PRE_PFEE":"",
    "VL_RECEITA_PFEE":"",
    "VL_PFEE_ACUMULADO":"",
    "QT_COTAS_TOTAL":"",
    "QT_COTAS_PF":"",
    "QT_COTAS_PJ":"",
    "QT_COTAS_TOTAL_ANTERIOR":"",
    "QT_COTAS_PF_ANTERIOR":"",
    "QT_COTAS_PJ_ANTERIOR":"",
    "VL_TAXA_ADMINISTRACAO":"",
    "VL_APLICACOES_TOTAL":"",
    "VL_APLICACOES_PF":"",
    "VL_APLICACOES_PJ":"",
    "VL_RESGATES_TOTAL":"",
    "VL_RESGATES_PF":"",
    "VL_RESGATES_PJ":"",
    "VL_RESGATES_APLIC_TOTAL":"",
    "VL_RESGATES_APLIC_PF":"",
    "VL_RESGATES_APLIC_PJ":"",
    "QT_COTAS_APLICACOES_TOTAL":"",
    "QT_COTAS_APLICACOES_PF":"",
    "QT_COTAS_APLICACOES_PJ":"",
    "QT_COTAS_RESGATES_TOTAL":"",
    "QT_COTAS_RESGATES_PF":"",
    "QT_COTAS_RESGATES_PJ":"",
    "NO_APLICACOES_TOTAL":"",
    "NO_APLICACOES_PF":"",
    "NO_APLICACOES_PJ":"",
    "NO_RESGATES_TOTAL":"",
    "NO_RESGATES_PF":"",
    "NO_RESGATES_PJ":"",
    "NO_COTISTAS_FECHAMENTO_TOTAL":"",
    "NO_COTISTAS_FECHAMENTO_PF":"",
    "NO_COTISTAS_FECHAMENTO_PJ":"",
    "VL_IOF_TOTAL":"",
    "VL_IR_TOTAL":"",
    "VL_RECEITA_SAQUE_CARENCIA":"",
    "VL_RECEITA_PENALTY":"",
    "VL_COTAS_EMITIR_ABERTURA":"",
    "VL_COTAS_RESGATAR_ABERTURA":"",
    "VL_COTAS_EMITIR_FECHAMENTO":"",
    "VL_COTAS_RESGATAR_FECHAMENTO":"",
    "VL_PFEE_RESGATADO":"",
    "VL_PATRIMONIO_FECHAMENTO_TOTAL":"",
    "VL_PATRIMONIO_FECHAMENTO_PF":"",
    "VL_PATRIMONIO_FECHAMENTO_PJ":"",
    "QT_COTAS_FECHAMENTO_TOTAL":"",
    "QT_COTAS_FECHAMENTO_PF":"",
    "QT_COTAS_FECHAMENTO_PJ":"",
    "VL_COTA_FECHAMENTO":self.vlr_cota_abertura,
    "VL_IOF_PF":"",
    "VL_IOF_PJ":"",
    "VL_IR_PF":"",
    "VL_IR_PJ":"",
    "VL_PFEE_CALCULADA_ATIVO":"",
    "QT_COTAS_APLIC_PF_RETROATIVA":"",
    "QT_COTAS_APLIC_PJ_RETROATIVA":"",
    "VL_APLIC_PF_RETROATIVA":"",
    "VL_APLIC_PJ_RETROATIVA":"",
    "NO_APLIC_PF_RETROATIVA":"",
    "NO_APLIC_PJ_RETROATIVA":"",
    "QT_COTAS_APLIC_PF_TRANSF":"",
    "QT_COTAS_APLIC_PJ_TRANSF":"",
    "VL_APLIC_PF_TRANSF":"",
    "VL_APLIC_PJ_TRANSF":"",
    "NO_APLIC_PF_TRANSF":"",
    "NO_APLIC_PJ_TRANSF":"",
    "VL_COTA_PRE_AMORTIZACAO":"",
    "VL_AMORTIZACAO_TOTAL":"",
    "VL_AMORTIZACAO_PJ":"",
    "VL_AMORTIZACAO_PF":"",
    "VL_COTA_AMORTIZACAO":"",
    "VL_TAXA_GESTAO":"",
    "CD_VERSAO":"",
    "CD_USUARIO":"",
    "DT_PROCESSADO":"",
    "DT_INICIO_PROCESSAMENTO":"",

    }
        BASE_DICT['CD_FUNDO'] = self.cdfundo 
        BASE_DICT['DT_POSICAO'] = self.dtposicao 
        BASE_DICT['IC_AF_POSICAO'] = self.afposicao
        BASE_DICT['VL_PATRIMONIO_ABERTURA'] = round(self.vlr_patrimonio_abertura,2)
        BASE_DICT['VL_COTA_ABERTURA'] = round(self.vlr_cota_abertura,8)
        BASE_DICT['VL_PATRIMONIO_FECHAMENTO_TOTAL'] = round(self.vlr_patrimonio_fechamento,2)
        BASE_DICT['QT_COTAS_FECHAMENTO_TOTAL'] = round(self.qtd_cotas_fechamento,8)
        BASE_DICT['VL_COTA_FECHAMENTO'] =  round(self.vlr_cota_fechamento,8)
        BASE_DICT["VL_COTA_PRE_PFEE"] = self.VALOR_PADRAO
        BASE_DICT["VL_PATRIMONIO_PRE_PFEE"] = self.VALOR_PADRAO
        BASE_DICT["VL_RECEITA_PFEE"] =  self.VALOR_PADRAO
        BASE_DICT["VL_PFEE_ACUMULADO"] =  self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_TOTAL"] = self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_PF"] =  self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_PJ"] = self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_TOTAL_ANTERIOR"] =  self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_PF_ANTERIOR"] = self.VALOR_PADRAO
        BASE_DICT["VL_TAXA_ADMINISTRACAO"] = self.VALOR_PADRAO
        BASE_DICT["VL_APLICACOES_TOTAL"] =self.VALOR_PADRAO
        BASE_DICT["VL_APLICACOES_PF"] =  self.VALOR_PADRAO
        BASE_DICT["VL_APLICACOES_PJ"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_TOTAL"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_PF"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_PJ"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_APLIC_TOTAL"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_APLIC_PF"] = self.VALOR_PADRAO
        BASE_DICT["VL_RESGATES_APLIC_PJ"] =self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_APLICACOES_TOTAL"] = self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_APLICACOES_PF"] = self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_APLICACOES_PJ"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_RESGATES_TOTAL"] =  self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_RESGATES_PF"] =self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_RESGATES_PJ"] = self.VALOR_PADRAO 
        BASE_DICT["NO_APLICACOES_TOTAL"] = self.VALOR_PADRAO 
        BASE_DICT["NO_APLICACOES_PF"] =self.VALOR_PADRAO
        BASE_DICT["NO_APLICACOES_PJ"] =self.VALOR_PADRAO
        BASE_DICT["NO_RESGATES_TOTAL"] = self.VALOR_PADRAO 
        BASE_DICT["NO_RESGATES_PF"] =  self.VALOR_PADRAO
        BASE_DICT["NO_RESGATES_PJ"] =self.VALOR_PADRAO
        BASE_DICT["NO_COTISTAS_FECHAMENTO_TOTAL"] =self.VALOR_PADRAO
        BASE_DICT["NO_COTISTAS_FECHAMENTO_PF"] =  self.VALOR_PADRAO
        BASE_DICT["NO_COTISTAS_FECHAMENTO_PJ"] =  self.VALOR_PADRAO
        BASE_DICT["VL_IOF_TOTAL"] =self.VALOR_PADRAO
        BASE_DICT["VL_IR_TOTAL"] =self.VALOR_PADRAO
        BASE_DICT["VL_RECEITA_SAQUE_CARENCIA"] =self.VALOR_PADRAO
        BASE_DICT["VL_RECEITA_PENALTY"] = self.VALOR_PADRAO 
        BASE_DICT["VL_COTAS_EMITIR_ABERTURA"] = self.VALOR_PADRAO
        BASE_DICT["VL_COTAS_RESGATAR_ABERTURA"] =  self.VALOR_PADRAO
        BASE_DICT["VL_COTAS_EMITIR_FECHAMENTO"] =  self.VALOR_PADRAO
        BASE_DICT["VL_COTAS_RESGATAR_FECHAMENTO"] =  self.VALOR_PADRAO
        BASE_DICT["VL_PFEE_RESGATADO"] =self.VALOR_PADRAO
        BASE_DICT["VL_PATRIMONIO_FECHAMENTO_PF"] = self.VALOR_PADRAO 
        BASE_DICT["VL_PATRIMONIO_FECHAMENTO_PJ"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_FECHAMENTO_PF"] =  self.VALOR_PADRAO
        BASE_DICT["QT_COTAS_FECHAMENTO_PJ"] = self.VALOR_PADRAO
        BASE_DICT["VL_IOF_PF"] = self.VALOR_PADRAO 
        BASE_DICT["VL_IOF_PJ"] =  self.VALOR_PADRAO
        BASE_DICT["VL_IR_PF"] = self.VALOR_PADRAO 
        BASE_DICT["VL_IR_PJ"] =  self.VALOR_PADRAO
        BASE_DICT["VL_PFEE_CALCULADA_ATIVO"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_APLIC_PF_RETROATIVA"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_APLIC_PJ_RETROATIVA"] =  self.VALOR_PADRAO
        BASE_DICT["VL_APLIC_PF_RETROATIVA"] = self.VALOR_PADRAO 
        BASE_DICT["VL_APLIC_PJ_RETROATIVA"] = self.VALOR_PADRAO 
        BASE_DICT["NO_APLIC_PF_RETROATIVA"] = self.VALOR_PADRAO 
        BASE_DICT["NO_APLIC_PJ_RETROATIVA"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_APLIC_PF_TRANSF"] = self.VALOR_PADRAO 
        BASE_DICT["QT_COTAS_APLIC_PJ_TRANSF"] =  self.VALOR_PADRAO
        BASE_DICT["VL_APLIC_PF_TRANSF"] =  self.VALOR_PADRAO
        BASE_DICT["VL_APLIC_PJ_TRANSF"] = self.VALOR_PADRAO 
        BASE_DICT["NO_APLIC_PF_TRANSF"] =  self.VALOR_PADRAO
        BASE_DICT["NO_APLIC_PJ_TRANSF"] =  self.VALOR_PADRAO
        BASE_DICT["VL_COTA_PRE_AMORTIZACAO"] =  self.VALOR_PADRAO
        BASE_DICT["VL_AMORTIZACAO_TOTAL"] = self.VALOR_PADRAO 
        BASE_DICT["VL_AMORTIZACAO_PJ"] = self.VALOR_PADRAO 
        BASE_DICT["VL_AMORTIZACAO_PF"] =  self.VALOR_PADRAO
        BASE_DICT["VL_COTA_AMORTIZACAO"] =  self.VALOR_PADRAO
        BASE_DICT["VL_TAXA_GESTAO"] = self.VALOR_PADRAO 
        return BASE_DICT



