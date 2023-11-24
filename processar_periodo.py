from movimentos_transfer import MovimentosTransfer
from datetime import datetime


# data_inicial = datetime(2023 , 6,  1  )
# data_final = datetime(2023 , 10 , 20 )


data_inicial = datetime(2022 , 8, 18  )
data_final = datetime(2022 , 8 , 22 )





movimentos_transfer = MovimentosTransfer('30991' , 'JIVE BOSSANOVA HIGH YIELD ADVISORY FIC FIM CP' ,  
                                        data_inicial ,  data_final)






# movimentos_transfer = MovimentosTransfer('20291_SEN01' , 'SUPPLIERCARD FIDC-SENIOR' ,  
#                                         data_inicial ,  data_final)

movimentos_transfer.atualizar_fundo()

# items = movimentos_transfer.get_itens_a_lancar(datetime(2023,3,20))

# movimentos_transfer.processarPeriodo(datetime(2023,6,30))