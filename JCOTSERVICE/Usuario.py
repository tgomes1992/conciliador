from .util import *



class Usuario():

    def __init__(self,codigo,nome,doc,tipo):
        self.codigo = codigo
        self.nome = nome
        self.tipo = tipo
        self.doc = doc


    def dados(self):
        if self.tipo =="J":
            return {
                "codigo": self.codigo , 
                'nome': self.nome,
                'cnpj': self.doc ,
                "cpf":"" ,
                'tipo': self.tipo
            }
        elif self.tipo=="F":
            return {
                "codigo": self.codigo , 
                'nome': self.nome,
                "cpf": self.doc , 
                "cnpj": "",
                'tipo': self.tipo
            }
     