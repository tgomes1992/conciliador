from .CotService import COTSERVICE
from bs4 import BeautifulSoup
import requests



class Cotista():
    pass

class Mancotistav2Service(COTSERVICE):

    url = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManCotistaV2Service"
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/ManCotistaV2Service"

    def body_habilitar_cotista_pco_xp(self , dados):
        '''Os dados são considerados um dicionário que realiza a habilitação do cotista PCO'''
        habilita_cotista = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
          	<soapenv:Header>
        		<wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
        			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        			<wsse:Username>{self.user}</wsse:Username>
        			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
        			</wsse:UsernameToken>
        		</wsse:Security>  
        	</soapenv:Header>   	
           <soapenv:Body>
              <tot:cadastrarCotistaV2Request>
                 <tot:cotista>         
        		      <tot:cdCotista>{dados['cd_cliente']}</tot:cdCotista>
                    <tot:cdOperador>2332886000104</tot:cdOperador>
                    <tot:cdDistribuidor>2332886000104</tot:cdDistribuidor>
                    <tot:cdCliente>{dados['cd_cliente']}</tot:cdCliente>
                    <tot:idTipoCotista>{dados['tipo_cotista']}</tot:idTipoCotista>
                    <tot:pcDevolucaoTxAdm>0.0</tot:pcDevolucaoTxAdm>
                    <tot:pcDevolucaoPerformance>0.0</tot:pcDevolucaoPerformance>
                    <tot:icSnExtrato>S</tot:icSnExtrato>
                    <tot:icSnDocFundo>N</tot:icSnDocFundo>
                    <tot:icSnAtivo>S</tot:icSnAtivo>
                    <tot:cdTipoEndereco>P</tot:cdTipoEndereco>
                    <tot:idClassificacao>0</tot:idClassificacao>
                    <tot:icSnNota>S</tot:icSnNota>
                    <tot:icSnInforme>S</tot:icSnInforme>
                    <tot:icSnCorrentista>S</tot:icSnCorrentista>
                    <tot:icSnAtivoLicenca>S</tot:icSnAtivoLicenca>
                    <tot:icSnReaplicaRepasse>N</tot:icSnReaplicaRepasse>
                    <tot:cdLiquidacaoRepasse>LI</tot:cdLiquidacaoRepasse>
                    <tot:cdClearingRepasse>STR</tot:cdClearingRepasse>
                    <tot:cdEmpresa>OT DTVM</tot:cdEmpresa>
                    <tot:icSnInvQualificado>N</tot:icSnInvQualificado>
                    <tot:icSnExtratoContaOrdem>{dados['c_ordem']}</tot:icSnExtratoContaOrdem>
                    <tot:icSnInformeContaOrdem>{dados['c_ordem']}</tot:icSnInformeContaOrdem>
                    <tot:dtInclusao>2022-05-12</tot:dtInclusao>
                    <tot:idRendimento>100</tot:idRendimento>
                    <tot:idFormaTributacao>10</tot:idFormaTributacao>
                    <tot:idBeneficiario>500</tot:idBeneficiario>
                    <tot:idAnbima>0</tot:idAnbima>
                    <tot:icSnContaTerceiros>N</tot:icSnContaTerceiros>
                    <tot:icSnAutorizaRepr>N</tot:icSnAutorizaRepr>
                    <tot:icSnExisteProcurador>N</tot:icSnExisteProcurador>
                    <tot:icSnCoTitularInforme>N</tot:icSnCoTitularInforme>
                    <tot:icSnInfConfiavel>S</tot:icSnInfConfiavel>
                    <tot:icSnLiberarAtuCadastral>N</tot:icSnLiberarAtuCadastral>
                    <tot:icTsCriterioResgateTotal>T</tot:icTsCriterioResgateTotal>
                    <tot:icSnCotistaDesenqAuto>N</tot:icSnCotistaDesenqAuto>
                    <tot:icSnFatca>N</tot:icSnFatca>
                    <tot:cdLimite>N</tot:cdLimite>
                    <tot:icSnEnvioEmailExtratoNota>N</tot:icSnEnvioEmailExtratoNota>
                    <tot:icSnInvProfissional>N</tot:icSnInvProfissional>
                    <tot:icSnAtivoAplicacao>S</tot:icSnAtivoAplicacao>
                    <tot:icSnAtivoResgate>S</tot:icSnAtivoResgate>  
                 </tot:cotista>
                 <!--Optional:-->
                 <glob:messageControl>
                    <glob:user>{self.user}</glob:user>
                    <glob:properties>
                       <!--Zero or more repetitions:-->
                       <glob:property name="?" value="?"/>
                    </glob:properties>
                 </glob:messageControl>
              </tot:cadastrarCotistaV2Request>
           </soapenv:Body>
        </soapenv:Envelope>'''
        return habilita_cotista

    def consultar_cotista(self, cd_cotista):
        '''cd_cotista é uma string com o código do cotista'''
        xmlcotista = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
          <soapenv:Header>
                {self.header_login()}
       	</soapenv:Header>
          <soapenv:Body>
             <tot:consultarCotistaV2Request>
                <tot:cotista>
                   <tot:cdCotista>{cd_cotista}</tot:cdCotista>
                </tot:cotista>
                <!--Optional:-->
                <glob:messageControl>
                   <glob:user>{self.user}</glob:user>
                   <glob:properties>
                      <!--Zero or more repetitions:-->
                      <glob:property name="?" value="?"/>
                   </glob:properties>
                </glob:messageControl>
             </tot:consultarCotistaV2Request>
          </soapenv:Body>
       </soapenv:Envelope>"""
        return xmlcotista

    def formatar_resposta(self , xml_text  , tag):
        soup = BeautifulSoup(xml_text , 'xml')
        resultado_consulta = soup.find(tag)
        return resultado_consulta.text

    def request_habilitar_pco_xp(self , dados):
        base_request = requests.post(self.url, self.body_habilitar_cotista_pco_xp(dados))
        return self.formatar_resposta(base_request.content ,  "ns2:code")

    def request_consultar_cotista(self , cd_cotista):
        base_request = requests.post(self.url, self.consultar_cotista(cd_cotista))
        return self.formatar_resposta(base_request.content , "ns3:code")


