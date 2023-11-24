from .CotService import COTSERVICE
import requests
from bs4 import BeautifulSoup
from datetime import datetime


'''incluir a classe de movimento jcot , na classe do responsável pelo serviço'''
class MovimentoJcot():
   pass





class MovimentoResumidoService(COTSERVICE):


    url = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/MovimentoResumidoService"
   
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/MovimentoResumidoService"

       
    def movimentoResumidoRequestBody(self,dados):
      base = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
	<soapenv:Header>
		<wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>  
	</soapenv:Header>
   <soapenv:Body>
      <tot:incluirMovimentoResumidoRequest>
         <tot:movimentoResumidoIncluir>
            <tot:dtMovimento>{dados['data']}</tot:dtMovimento>
            <tot:cdTipoMov>{dados['tipo']}</tot:cdTipoMov>
            <tot:cdCotista>{dados['cotista']}</tot:cdCotista>
            <tot:cdFundo>{dados['fundo']}</tot:cdFundo>
            <tot:cdCriterioResgate>F</tot:cdCriterioResgate>
            <tot:cdFormaLiquidacao>{dados['liquidacao']}</tot:cdFormaLiquidacao>
            <tot:vlDigitado>{dados['valor']}</tot:vlDigitado>
            <tot:qtCotasDigitada>{dados['qtdcotas']}</tot:qtCotasDigitada>
            <tot:cdClearing>{dados['clearing']}</tot:cdClearing>
         </tot:movimentoResumidoIncluir>
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
      </tot:incluirMovimentoResumidoRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
      return base


    def formatar_resposta(self ,  xml_resposta , cotista ,  data , fundo):
        soup = BeautifulSoup(xml_resposta, 'xml')
        return  {
            "fundo": fundo , 
            "cotista": cotista , 
            "codigo_resposta" : soup.find('ns3:code').text ,
            "descricao" : soup.find('ns3:desc').text ,
            "execucao" :  datetime.today().strftime("%Y-%m-%d %H:%M:%S") , 
            "data_lancamento": data 
            
        }


    def movimentoResumidoRequest(self , dados):
        base_request = requests.post(self.url, self.movimentoResumidoRequestBody(dados))
        return self.formatar_resposta(base_request.content ,  dados['cotista'] , dados["data"] , dados['fundo'])


    def movimentoResumidoRequestpp(self , dados):
        base_request = requests.post(self.url_pp, self.movimentoResumidoRequestBody(dados))
        # print (base_request.content)
        return self.formatar_resposta(base_request.content)


