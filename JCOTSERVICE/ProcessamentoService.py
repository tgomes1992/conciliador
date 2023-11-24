from .CotService import COTSERVICE
import requests
from bs4 import BeautifulSoup



class ProcessamentoService(COTSERVICE):


    url = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/FundosService"
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/FundosService"

    def ProcessarRequestBody(self , dados_processamento):
        xml_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
   <soapenv:Header>
   {self.header_login()}
   </soapenv:Header>
   <soapenv:Body>
      <tot:processarRequest>
         <!--Optional:-->
         <tot:processar>
            <tot:cdFundo>{dados_processamento['cd_fundo']}</tot:cdFundo>
            <tot:dtProcessamento>{dados_processamento['data']}</tot:dtProcessamento>
            <tot:icAfProcesso>{dados_processamento['tipo']}</tot:icAfProcesso>
            <!--Optional:-->
            <tot:vlCotaBruta>?</tot:vlCotaBruta>
         </tot:processar>
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
      </tot:processarRequest>
   </soapenv:Body>
</soapenv:Envelope>'''

        return xml_request


    def ConsultarJobProcessamentoBody(self,  id_job):
      body = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
   <soapenv:Header>
            {self.header_login()}
   </soapenv:Header>
   <soapenv:Body>
      <tot:consultarJobRequest>
         <!--Optional:-->
         <tot:consultarJob>
            <tot:idJob>{id_job}</tot:idJob>
         </tot:consultarJob>
         <!--Optional:-->
         <glob:messageControl>
            <glob:user>{self.user}</glob:user>
            <glob:properties>
               <!--Zero or more repetitions:-->
               <glob:property name="?" value="?"/>
            </glob:properties>
         </glob:messageControl>
      </tot:consultarJobRequest>
   </soapenv:Body>
</soapenv:Envelope>'''

    def get_job_processamento(self,xml_body):
        soup = BeautifulSoup(xml_body ,  'xml')
        pass
    


    def ProcessarRequest(self , dados_processamento):
        base_request = requests.post(self.url, self.ProcessarRequestBody(dados_processamento))
        print (base_request.content)

   
    def ProcessarRequestPP(self , dados_processamento):
        base_request = requests.post(self.url, self.ProcessarRequestBody(dados_processamento))
        print (base_request.content)


    def ConsultarJobRequest(self , job):
        base_request = requests.post(self.url, self.ConsultarJobProcessamentoBody(job))
        print (base_request.content)

