from .ext_days import day_path
import os


SERVER_NAME = "SCOTOTRJ01"


DATA_PATH = f"//{SERVER_NAME}/H/CUSTODIA/7 Escrituração de Ativos/1 - Controle das Operações/Batimentos/data_files/"


BATIMENTOS_FOLDER = f"//{SERVER_NAME}/H/CUSTODIA/7 Escrituração de Ativos/1 - Controle das Operações/Batimentos/{day_path()}"


def criar_pastas():
    if not os.path.exists(BATIMENTOS_FOLDER):
        try:
            os.mkdir(BATIMENTOS_FOLDER[:-12])
        except:
            pass
        try:
            os.mkdir(BATIMENTOS_FOLDER[:-9])
        except:
            pass
        try:
            os.mkdir(BATIMENTOS_FOLDER)
        except:
            pass





CWD = os.getcwd()


