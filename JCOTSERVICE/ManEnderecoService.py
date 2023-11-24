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


class ManEnderecoService(COTSERVICE):

    url  = "https://oliveiratrust.totvs.amplis.com.br:443/jcotserver/services/ManEnderecoService"
    url_pp = "https://oliveiratrust-pp.totvs.amplis.com.br:443/jcotserver/services/ManEnderecoService"
    data = date.today().strftime("%Y-%m-%d")

    def consultar_endereco(self , codigo_cliente):
        body = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tot="http://totvs.cot.webservices" xmlns:glob="http://totvs.cot.webservices/global">
   <soapenv:Header>
            {self.header_login()}
   </soapenv:Header>
   <soapenv:Body>
      <tot:consultarEnderecoRequest>
         <tot:endereco>
            <tot:cdCliente>{codigo_cliente}</tot:cdCliente>
         </tot:endereco>
         <!--Optional:-->
         <glob:messageControl>
            <glob:user>{self.user}</glob:user>
            <glob:properties>
               <!--Zero or more repetitions:-->
               <glob:property name="?" value="?"/>
            </glob:properties>
         </glob:messageControl>
      </tot:consultarEnderecoRequest>
   </soapenv:Body>
</soapenv:Envelope>'''
        return body

    def body_cadastrar_endereco(self,dados):
        cadastro_clientes = f'''a'''
        return cadastro_clientes

    def alterarEndereco(self,dados):
        corpo_alteracao_cotistas = f'''a'''
        return corpo_alteracao_cotistas

    def formatar_resposta(self , xml_text , cliente):
        soup = BeautifulSoup(xml_text , 'xml')
        resultado_consulta = soup.find_all("ns2:endereco")
        try:
            e_mails_cadastrados = [ item['ns2:dsEmail'].text for item in resultado_consulta if item.find("ns2:dsEmail") ]
            return {
                "cliente": cliente,
                "e-mails": ",".join(e_mails_cadastrados)
            }

        except Exception as e:
            print (e)
            return {
                "cliente": cliente ,
                "e-mails": "Sem E-mail Cadastrados"
            }

    def request_consultar_endereco(self,codigo_cliente):
        base_request = requests.post(self.url, self.consultar_endereco(codigo_cliente))
        return self.formatar_resposta(base_request.content , codigo_cliente)







