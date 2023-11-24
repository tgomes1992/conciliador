from datetime import date, datetime
from .Usuario import Usuario
from .PosicaodoFundo import PosicaodoFundo
from .PosicaoNotaHistorica import PosicaoNotaHistorica
from .PosicaoRendimentoHistorico import PosicaoRendimentoHistorico
from .Movimento import Movimento
from .Resgate import Resgate
from .ListFundosService import ListFundosService
from .MovimentoResumisoService import MovimentoResumidoService
from .RelPosicaoFundoCotistaService  import RelPosicaoFundoCotistaService
from .ProcessamentoService import ProcessamentoService
from .ManClienteService import ManClienteService
from .ManCotistaV2Service import Mancotistav2Service
from .ManEnderecoService import ManEnderecoService









class JCOTSERVICE():

    data = date.today().strftime("%Y-%m-%d")

    cotservices = {
         "cadastrar_cliente": "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManClienteService",
         "habilitar_cotista": "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManCotistaV2Service",
         "movimento": "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/MovimentoResumidoService" , 
         "consulta_cotista": "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManCotistaV2Service" ,
         "list_fundos" :  "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/FundosService" , 
         "list_posicoes":  "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/RelPosicaoFundoCotistaService"
      }


    def __init__(self):
        self.user = "thiago"
        self.password = "Senh@123"
        
       

    def cadastrarCliente(self,dados , data):
        teste = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
	   <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>  
   	</soapenv:Header>
   <soapenv:Body>
      <tot:cadastrarClienteRequest>
         <tot:cliente>             
            <tot:cdCliente>{dados['codigo']}</tot:cdCliente>
            <tot:icFjPessoa>{dados['tipo']}</tot:icFjPessoa>
            <tot:idEstadoCivil>0</tot:idEstadoCivil>
            <tot:idConstituicao>0</tot:idConstituicao>
            <tot:idRegimeCasamento>0</tot:idRegimeCasamento>
            <tot:idEscolaridade>0</tot:idEscolaridade>
            <tot:nmCliente>{dados['nome']}</tot:nmCliente>
            <tot:nmAbreviado>{dados['nome'][0:19]}</tot:nmAbreviado>
            <tot:icSnBrasileiro>S</tot:icSnBrasileiro>
            <tot:noCgc>{dados["cnpj"]}</tot:noCgc>
            <tot:noCpf>{dados["cpf"]}</tot:noCpf>
            <tot:dsDomicilio>BRASIL</tot:dsDomicilio>
            <tot:nmContato>{dados["codigo"]}</tot:nmContato>
            <tot:cdUsuarioInclusao>{self.user}</tot:cdUsuarioInclusao>
            <tot:dtInclusao>{data}</tot:dtInclusao>
            <tot:icSnExposta>N</tot:icSnExposta>
            <tot:dtUltRenovacao>{data}</tot:dtUltRenovacao>
            <tot:icSnOrigemBvmf>N</tot:icSnOrigemBvmf>
         </tot:cliente>
         <!--Optional:-->
         <glob:messageControl>
            <glob:user>{self.user}</glob:user>
         </glob:messageControl>
      </tot:cadastrarClienteRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
        return teste

    def consultaCliente(self,dados):
        teste = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
	   <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>  
   	</soapenv:Header>      
         <soapenv:Body>
            <tot:consultarClienteRequest>
               <tot:cliente>
                  <tot:cdCliente>{dados}</tot:cdCliente>
                  <!--Optional:-->
                  <tot:noCpf>?</tot:noCpf>
                  <!--Optional:-->
                  <tot:noCgc>?</tot:noCgc>
               </tot:cliente>
               <!--Optional:-->
               <glob:messageControl>
                  <glob:user>{self.user}</glob:user>
                  <glob:properties>
                     <!--Zero or more repetitions:-->
                     <glob:property name="?" value="?"/>
                  </glob:properties>
               </glob:messageControl>
            </tot:consultarClienteRequest>
         </soapenv:Body>
</soapenv:Envelope>'''
        return teste


    def alterarCliente(self,dados):
        teste = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
	   <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>  
   	</soapenv:Header>
   <soapenv:Body>
      <tot:alterarClienteRequest>
         <tot:cliente>             
            <tot:cdCliente>{dados['codigo']}</tot:cdCliente>
            <tot:icFjPessoa>{dados['tipo']}</tot:icFjPessoa>
            <tot:idEstadoCivil>0</tot:idEstadoCivil>
            <tot:idConstituicao>0</tot:idConstituicao>
            <tot:idRegimeCasamento>0</tot:idRegimeCasamento>
            <tot:idEscolaridade>0</tot:idEscolaridade>
            <tot:nmCliente>{dados['nome']}</tot:nmCliente>
            <tot:nmAbreviado>{dados['nome'][0:19]}</tot:nmAbreviado>
            <tot:icSnBrasileiro>S</tot:icSnBrasileiro>
            <tot:noCgc>{dados["cnpj"]}</tot:noCgc>
            <tot:noCpf>{dados["cpf"]}</tot:noCpf>
            <tot:dsDomicilio>BRASIL</tot:dsDomicilio>
            <tot:nmContato>{dados["codigo"]}</tot:nmContato>
            <tot:cdUsuarioInclusao>{self.user}</tot:cdUsuarioInclusao>
            <tot:dtInclusao>{self.data}</tot:dtInclusao>
            <tot:icSnExposta>N</tot:icSnExposta>
            <tot:dtUltRenovacao>{self.data}</tot:dtUltRenovacao>
            <tot:icSnOrigemBvmf>N</tot:icSnOrigemBvmf>
         </tot:cliente>
         <!--Optional:-->
         <glob:messageControl>
            <glob:user>{self.user}</glob:user>
         </glob:messageControl>
      </tot:alterarClienteRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
        return teste


    def habilitar_cotista(self,dados):
        teste = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
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
      </tot:cadastrarCotistaV2Request>
   </soapenv:Body>
</soapenv:Envelope>'''
        return teste

    def movimento_cotista(self,dados):
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

    def excluir_movimentacao(self,cod_movimento):
       xmlexcluirmov = f'''
       <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
         <soapenv:Header>
            <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
               <wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
               <wsse:Username>{self.user}</wsse:Username>
               <wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
               </wsse:UsernameToken>
            </wsse:Security>  
         </soapenv:Header>
         <soapenv:Body>
            <tot:excluirMovimentoResumidoRequest>
               <tot:movimentoResumidoExcluir>
                  <tot:idNota>{cod_movimento}</tot:idNota>
               </tot:movimentoResumidoExcluir>
               <!--Optional:-->
               <glob:messageControl>
                  <glob:user>thiago</glob:user>
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
            </tot:excluirMovimentoResumidoRequest>
         </soapenv:Body>
      </soapenv:Envelope>'''
       return xmlexcluirmov

    def consultar_cotista(self,cd_cotista):
      xmlcotista = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
	   <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>  
   	</soapenv:Header>
      <soapenv:Body>
         <tot:consultarCotistaV2Request>
            <tot:cotista>
               <tot:cdCotista>{cd_cotista}</tot:cdCotista>
            </tot:cotista>
            <!--Optional:-->
            <glob:messageControl>
               <glob:user>thiago</glob:user>
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
         </tot:consultarCotistaV2Request>
      </soapenv:Body>
   </soapenv:Envelope>"""
      return xmlcotista

    def list_fundos(self):
      xml_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices">
   <soapenv:Header>
            <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <wsse:Username>{self.user}</wsse:Username>
            <wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
            </wsse:UsernameToken>
            </wsse:Security>   
   </soapenv:Header>
   <soapenv:Body>
      <tot:ListFundosRequest>
         <tot:fundo>
            <tot:login>{self.user}</tot:login>
         </tot:fundo>
      </tot:ListFundosRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
      return xml_request

    def list_posicao_fundos(self,fundo):
      xml_request = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
   <soapenv:Header>
         <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
         <wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
         <wsse:Username>{self.user}</wsse:Username>
         <wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
         </wsse:UsernameToken>
         </wsse:Security>   
   </soapenv:Header>
   <soapenv:Body>
      <tot:obterRelPosFundoCotistaRequest>
         <tot:filtro>
            <tot:cdFundo>{fundo['codigo']}</tot:cdFundo>   
            <tot:dtPosicao>{fundo['dataposicao']}</tot:dtPosicao>
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
    


cot = JCOTSERVICE()

