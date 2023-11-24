from .util import *



class PosicaoRendimentoHistorico():
   

    LISTA = []

    def __init__(self,nota,dtposicao,rendimento,
            fundo,cotista,aliquota,vlr_ir):
        self.idnota = nota
        self.dt_posicao = dtposicao
        self.cd_rendimento = "RA"
        self.vl_rendimento = strtofloat(rendimento)
        self.cd_fundo =  fundo
        self.cd_cotista = cotista
        self.aliquota_ir = aliquota.replace("%","")
        self.vlr_ir = strtofloat(vlr_ir)


    def get_base_dict(self):
        BASEDICT = {}
        BASEDICT['ID_NOTA'] =  self.idnota
        BASEDICT['DT_POSICAO'] =  self.dt_posicao
        BASEDICT['CD_RENDIMENTO'] =  self.cd_rendimento
        BASEDICT['VL_RENDIMENTO'] =  self.vl_rendimento
        BASEDICT['PC_ALIQUOTA'] =  self.aliquota_ir
        BASEDICT['VL_IR'] =  self.vlr_ir
        BASEDICT['CD_DESENQ'] = ""    
        BASEDICT['ID_DESENQ'] = 0
        BASEDICT['IC_FC_DESENQ'] = "F"
        BASEDICT['CD_ALIQUOTA_ORIGEM'] =  ""
        BASEDICT['CD_FUNDO'] =  self.cd_fundo
        BASEDICT['CD_COTISTA'] =  self.cd_cotista
        return BASEDICT

