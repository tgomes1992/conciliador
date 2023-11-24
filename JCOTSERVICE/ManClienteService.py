from .CotService import COTSERVICE
import requests
from bs4 import BeautifulSoup
from datetime import date


class Cliente():


    def __init__(self , nome , cdCliente , tipoPessoa):
    

        self.cdCliente= cdCliente
        self.icFjPessoa= tipoPessoa
        self.idEstadoCivil= "0"
        self.idConstituicao= "0"
        self.idRegimeCasamento = "0"
        self.idEscolaridade = "0"
        self.nmCliente = nome[0:15]
        self.nmAbreviado = nome[0:15]
        # self.noCpf = 22597297200
        self.icSnBrasileiro= "S"
        self.icMfSexo= "M"
        self.icSnExposta= "N"
        # self.cdOcupacaoProfissional= 2410-05



    def definir_pessoa(self):
        pass



class ManClienteService(COTSERVICE):

    url  = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManClienteService"
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/ManClienteService"
    data = date.today().strftime("%Y-%m-%d")

    def consultar_body(self , codigo_cliente):
        body = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
        <soapenv:Header>
                {self.header_login()}
        </soapenv:Header>
       <soapenv:Body>
          <tot:consultarClienteRequest>
             <tot:cliente>
                <tot:cdCliente>{codigo_cliente}</tot:cdCliente>
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
    </soapenv:Envelope> '''
        return body

    def body_cadastrar_cliente(self,dados ):
        cadastro_clientes = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
    <soapenv:Header>
            {self.header_login()}
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
      </tot:cadastrarClienteRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
        return cadastro_clientes

    def consultaCliente(self,cd_cliente):
        '''base para o cadastro dos clientes'''
        consulta_clientes = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
            {self.header_login()}
   	</soapenv:Header>      
         <soapenv:Body>
            <tot:consultarClienteRequest>
               <tot:cliente>
                  <tot:cdCliente>{cd_cliente}</tot:cdCliente>
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
        return consulta_clientes

    def alterarCliente(self,dados):
        '''body base para realizar a alteração de clientes,'''
        corpo_alteracao_cotistas = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
      <soapenv:Header>
            {self.header_login()}
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
        return corpo_alteracao_cotistas

    def formatar_resposta(self , xml_text):
        soup = BeautifulSoup(xml_text , 'xml')
        resultado_consulta = soup.find("ns3:code")
        return resultado_consulta.text

    def request_consultar_cliente(self,codigo_cliente):
        base_request = requests.post(self.url, self.consultar_body(codigo_cliente))
        return self.formatar_resposta(base_request.content)

    def request_cadastrar_clientes(self,dados):
        base_request = requests.post(self.url, self.body_cadastrar_cliente(dados))
        return base_request.content





