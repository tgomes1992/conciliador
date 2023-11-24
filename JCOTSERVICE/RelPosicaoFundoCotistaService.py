from .CotService import COTSERVICE
import requests
from bs4 import BeautifulSoup
import pandas as pd


class RelPosicaoFundoCotistaService(COTSERVICE):
    url = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/RelPosicaoFundoCotistaService"
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/RelPosicaoFundoCotistaService"

    '''o fundo é sempre um dicionário com o código do cun'''

    def relPosicaoFundoCotistaBody(self, fundo):
        xml_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
   <soapenv:Header>
                {self.header_login()}
   </soapenv:Header>
   <soapenv:Body>
      <tot:obterRelPosFundoCotistaRequest>
         <tot:filtro>
            <tot:cdFundo>{fundo['codigo']}</tot:cdFundo>   
            <tot:dtPosicao>{fundo['dataPosicao']}</tot:dtPosicao>
         </tot:filtro>
         <!--Optional:-->
         <glob:messageControl>
            <glob:user>{self.user}</glob:user>
            <!--Optional:-->
            <glob:sessionID>?</glob:sessionID>
            <!--Optional:-->
            <glob:messageID>?</glob:messageID>
            <glob:remoteAddr>?</glob:remoteAddr>
            <!--Optional:-->
            <glob:channel>?</glob:channel>
            <!--Optional:-->
            <glob:properties>
               <!--Zero or more repetitions:-->
               <glob:property name="?" value="?"/>
            </glob:properties>
         </glob:messageControl>
      </tot:obterRelPosFundoCotistaRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
        return xml_request

    def RelPosicaoFundoCotistaServiceRequest(self, fundo):
        base_request = requests.post(self.url, self.relPosicaoFundoCotistaBody(fundo))
        print(fundo)
        #   print (base_request.content)
        return base_request.content

    def get_status(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)
        soup = BeautifulSoup(xml, "xml")
        try:
            status = soup.find("ns2:statusFundo").text
            return status
        except:
            return "Fundo não disponível para consulta"

    def get_cotistas(self, xml_part):
        cotistas = xml_part.find_all("ns2:cotista")
        return cotistas

    def get_posicoes_cotistas(self, xml_part):
        total_cotistas = xml_part.find("ns2:totalCotista")

        posicao = {
            "cd_cotista": xml_part.find("ns2:cdCotista").text,
            "nmCotista": xml_part.find("ns2:nmCotista").text,
            # "cpfcnpjCotista": xml_part.find("ns2:cpfcnpjCotista").text,
            "qtCotas": float(total_cotistas.find("ns2:qtCotas").text),
            "vlAplicacao": float(total_cotistas.find("ns2:vlAplicacao").text),
            "vlCorrigido": float(total_cotistas.find("ns2:vlCorrigido").text),
            "vlIof": float(total_cotistas.find("ns2:vlIof").text),
            "vlIr": float(total_cotistas.find("ns2:vlIr").text),
            "vlResgate": float(total_cotistas.find("ns2:vlResgate").text),
            "vlRendimento": float(total_cotistas.find("ns2:vlRendimento").text),
        }

        return posicao

    def get_cd_cotistas(self, xml):
        cd_cotista = xml.find("ns2:cdCotista").text.strip()
        return cd_cotista

    def get_lista_cotistas(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)
        soup = BeautifulSoup(xml, "xml")
        cotistas = self.get_cotistas(soup)
        lista_cotistas = [{"cotista": self.get_cd_cotistas(item)} for item in cotistas]
        return lista_cotistas

    def get_posicoes(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)
        soup = BeautifulSoup(xml, "xml")
        cotistas = self.get_cotistas(soup)
        posicoes = [self.get_posicoes_cotistas(item) for item in cotistas]
        return posicoes

    def get_posicoes_table(self, fundo):
        base_dados = self.get_posicoes(fundo)
        df = pd.DataFrame.from_dict(base_dados)
        df['dataPosicao'] = fundo['dataPosicao']
        return df

    def get_posicoes_json(self, fundo):
        return self.get_posicoes(fundo)

    def get_posicao_fundo(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)
        try:
            soup = BeautifulSoup(xml, 'xml')
            total_fundo = soup.find("ns2:totalFundos")
            valor = {
                "fundo": fundo['codigo'],
                "valor": float(total_fundo.find("ns2:qtCotas").text)
            }
        except:
            valor = {
                "fundo":  fundo['codigo'],
                "valor":  0
            }
        return valor

    def get_qtd_fundo(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)
        try:
            soup = BeautifulSoup(xml, 'xml')

            total_fundo = soup.find("ns2:totalFundos")
            valor = float(total_fundo.find("ns2:qtCotas").text)
        except:
            valor = 0
        return valor

    def get_posicao_consolidada(self, fundo):
        xml = self.RelPosicaoFundoCotistaServiceRequest(fundo)

        try:
            soup = BeautifulSoup(xml, 'xml')
            total_fundo = soup.find("ns2:totalFundos")
            dict_base = {
                "qtCotas": float(total_fundo.find("ns2:qtCotas").text),
                "vlAplicacao": float(total_fundo.find("ns2:vlAplicacao").text),
                "vlCorrigido": float(total_fundo.find("ns2:vlCorrigido").text),
                "vlIof": float(total_fundo.find("ns2:vlIof").text),
                "vlIr": float(total_fundo.find("ns2:vlIr").text),
                "vlResgate": float(total_fundo.find("ns2:vlResgate").text),
                "vlRendimento": float(total_fundo.find("ns2:vlRendimento").text)
            }


        except:
            dict_base = {
                "qtCotas": 0,
                "vlAplicacao": 0,
                "vlCorrigido": 0,
                "vlIof": 0,
                "vlIr": 0,
                "vlResgate": 0,
                "vlRendimento": 0
            }

        return dict_base




