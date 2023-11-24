class COTSERVICE():

   def __init__(self , user , password ):
      self.user = user
      self.password = password



   def header_login(self):
      wsse_login  = f'''<wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsse:UsernameToken wsu:Id="UsernameToken-1" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsse:Username>{self.user}</wsse:Username>
			<wsse:Password Typ="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{self.password}</wsse:Password>
			</wsse:UsernameToken>
		</wsse:Security>'''
      return wsse_login

   
   '''daqui em diante , cada um dos serviços deve ter os seus próprios métodos de acordo com o respectivo wsdl'''

