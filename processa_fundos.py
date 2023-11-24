import os 



job  =  {"codigo": "1944" ,  "data_processamento":  "2023-07-03",  "dataPosicao": "2023-07-03" , "cota_processamento" :  1.0,  "tipo": "abertura" ,"status": "FECHADO" }


string_cli = f"-codigo {job['codigo']} -dataProcessamento {job['data_processamento']} -cota_processamento {job['cota_processamento']} -dataPosicao {job['dataPosicao']} -tipo {job['tipo']} -status {job['status']}"


print (string_cli)


# os.system(f"processamentoAutomatico {string_cli}")


