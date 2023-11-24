from .util import *
import pandas as pd


class PosicaodoRendimento():
    BASEDICT = {}

    LISTA = []

    def __init__(self,nota,dtposicao,rendimento,
            fundo,cotista,aliquota,vlr_ir):
        self.idnota = nota
        self.dt_posicao = dtposicao
        self.cd_rendimento = "RA"
        self.vl_rendimento = rendimento 
        self.cd_fundo =  fundo
        self.cd_cotista = cotista
        self.aliquota_ir = aliquota
        self.vlr_ir = vlr_ir



    def get_base_dict(self):
        self.BASEDICT['ID_NOTA'] =  self.idnota
        self.BASEDICT['DT_POSICAO'] =  self.dt_posicao
        self.BASEDICT['CD_RENDIMENTO'] =  self.cd_rendimento
        self.BASEDICT['VL_RENDIMENTO'] =  self.cd_rendimento
        self.BASEDICT['PC_ALIQUOTA'] =  self.aliquota_ir
        self.BASEDICT['VL_IR'] =  self.vlr_ir
        self.BASEDICT['CD_DESENQ'] = 0    
        self.BASEDICT['ID_DESENQ'] = 0
        self.BASEDICT['IC_FC_DESENQ'] = 0 
        self.BASEDICT['CD_ALIQUOTA_ORIGEM'] =  ""
        self.BASEDICT['CD_FUNDO'] =  self.cd_fundo
        self.BASEDICT['CD_COTISTA'] =  self.cd_cotista
        return self.BASEDICT

    


