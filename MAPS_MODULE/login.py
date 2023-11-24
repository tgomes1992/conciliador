from .constants import SERVER_NAME
import pandas as pd




def get_login():
    login = pd.read_excel(f"//{SERVER_NAME}/H/CUSTODIA/7 Escrituração de Ativos/1 - Controle das Operações/Batimentos/data_files/login.xlsx")
    logins = {
        'user' : login.user.values[0]  , 
        'senha': login.password.values[0]
    }
    return logins



