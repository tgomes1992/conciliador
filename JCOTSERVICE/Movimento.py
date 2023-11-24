from .util import *
from datetime import datetime , date

class Movimento():

        def __init__(self,idnota,cdpadrao,dtmovimento,tip_movimento,cd_cotista,
        cdfundo,dt_liquidacao,vlr_bruto,vlr_ir,vlr_iof,vlr_liqudo,qtd_cotas):
                self.idnota = idnota
                self.cdpadrao = cdpadrao
                self.dtmovimento = dtmovimento
                self.cdtipo = tip_movimento
                self.cd_cotista = cd_cotista
                self.cd_fundo = cdfundo
                self.cd_criterio_resgate = "F"
                self.cd_liquidacao = "DI"
                self.dt_liquidacao = dt_liquidacao
                self.vlr_bruto  = vlr_bruto
                self.vlr_ir = vlr_ir
                self.vlr_iof = vlr_iof
                self.vlr_liquido = vlr_liqudo
                self.qtd_cotas = qtd_cotas
                self.ic_sn_emitida = "N"
                self.cd_clearing = "STR"
                self.ic_air_liq = "R"
                self.ic_sn_mov_res = "N"
                self.ic_sn_neg = "N"
                self.ic_sn_zer  =  "N"
                self.ic_sn_env = "N"
                self.ic_sn_rend = "N"
                self.ic_sn_limite = "S"
                self.ic_sn_sem_ag = "N"
                self.ic_sn_recalcula = "N"
                self.ic_sn_distr_rendimento = "N"
                self.ic_sn_penalty = "N"
                self.ic_sn_colagem = "N"
                self.ic_sn_recalcula_dt = "N"


        def get_base_dict(self):
                base_dict = { 
                "ID_NOTA":self.idnota,
                "CD_PADRAO":self.cdpadrao,
                "DT_MOVIMENTO":datetime.strptime(str(self.dtmovimento),"%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y"),
                "DT_RETROATIVO_ESTORNO":"",
                "CD_TIPO":self.cdtipo,
                "CD_COTISTA":self.cd_cotista,
                "CD_FUNDO":self.cd_fundo,
                "DT_DIGITACAO":"",
                "VL_DIGITADO":"",
                "QT_COTAS_DIGITADA":"",
                "ID_NOTA_DIGITADA":"",
                "CD_CRITERIO_RESGATE":self.cd_criterio_resgate,
                "CD_LIQUIDACAO":self.cd_liquidacao,
                "ID_BANCO":"",
                "CD_AGENCIA_EXTERNA":"",
                "CD_CONTA_EXTERNA":"",
                "DT_LIQUIDACAO_FISICA":datetime.strptime(str(self.dt_liquidacao),"%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y"),
                "DT_LIQUIDACAO_FINANCEIRA":datetime.strptime(str(self.dt_liquidacao),"%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y"),
                "VL_BRUTO": abs(strtofloat(self.vlr_bruto)),
                "VL_IR": abs(strtofloat(self.vlr_ir)),
                "VL_IOF":abs(strtofloat(self.vlr_iof)),
                "VL_LIQUIDO":abs(strtofloat(self.vlr_liquido)),
                "VL_PERFORMANCE":"",
                "QT_COTAS":abs(strtofloat(self.qtd_cotas)),
                "CD_USUARIO_DIGITACAO":"",
                "CD_USUARIO_CONFERENTE":"",
                "CD_USUARIO_AUTORIZANTE":"",
                "IC_SN_NOTA_EMITIDA":self.ic_sn_emitida,
                "ID_NOTA_ORIGEM":"",
                "DT_TRANSFERENCIA":"",
                "DS_HISTORICO":"",
                "CD_CLEARING":self.cd_clearing,
                "IC_AR_LIQUIDACAO":self.ic_air_liq,
                "IC_SN_CPMF":"",
                "VL_CPMF":"",
                "IC_SN_CONFERIDO":"",
                "DT_CONFERENCIA":"",
                "VL_PENALTY":"",
                "VL_COTA_MOVIMENTACAO":"",
                "CD_CUSTODIA":"",
                "CD_INTERFACE_1":"",
                "ID_BANCO_TERCEIROS":"",
                "CD_AGENCIA_TERCEIROS":"",
                "CD_CONTA_TERCEIROS":"",
                "CD_CONTA_ORDEM":"",
                "IC_SN_ENVIADO":"",
                "CD_DIAS_LIQ_RESGATE":"",
                "ID_SAC_DIVIDENDO":"",
                "CD_IF":"",
                "CD_TIPO_TRANSFERENCIA":"",
                "IC_SN_MOV_RESTAURADO":self.ic_sn_mov_res,
                "DT_LIQ_FINAN_ANTECIPADA":"",
                "IC_SN_NEGOCIACAO":self.ic_sn_neg,
                "IC_SN_ZERAGEM":self.ic_sn_zer,
                "IC_SN_ENVIADO_EMAIL":self.ic_sn_env,
                "IC_SN_COLAGEM":self.ic_sn_colagem,
                "IC_SN_DISTR_RENDIMENTO":self.ic_sn_distr_rendimento,
                "IC_SN_LIMITE_GLOBAL":self.ic_sn_distr_rendimento,
                "IC_SN_PENALTY_SEM_AGEND":self.ic_sn_penalty,
                "IC_SN_RECALCULA_DT_FIS_FINAN":self.ic_sn_recalcula_dt
                }
                return base_dict
