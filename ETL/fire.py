from etl.remote_database import RemoteDataBase
from etl.utils import (
    normalize_monetary_value,
    normalize_date,
    date_to_datetime,
)

import csv
import datetime

class InfobrasilDataBase(RemoteDataBase):
    def __init__(self,nomeArq, **kwargs):
        self.nomeArq = nomeArq
        super().__init__(**kwargs)
        

    def process_data(self):
        # create connection to firebird
        firebird = self.connect_firebird()
        # connection firebird
        firebird.connect()
  

        days = (date_to_datetime(self.end_date) - date_to_datetime(self.start_date)).days

        # In cases custon query
        if self.query:
            query = self.query

        print(firebird)

        # # set query to cursor
        firebird.cursor.execute(query)
        results = firebird.cursor.fetchall()
  
        with open(f'./files/AS_{self.nomeArq}{datetime.date.today() - datetime.timedelta(1)}.csv','w', encoding='utf-8') as csvfile:
            fwrite = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
            fwrite.writerow(["Data da Transação",
                        "Cód. Loja",
                        "Cód. PDV",
                        "Cód. Ticket",
                        "Total da Compra",
                        "Cód. Consumidor",
                        "Forma de Pagamento",
                        "EAN",
                        "Descritivo Item",
                        "Quantidade",
                        "Valor Total",
                        "Desconto / Acrescimo - Item",
                        "Desconto / Acrescimo - Total Ticket"])

            for result in results:
                data_transacao = str(result[0])
                cod_loja = str(result[1])
                cod_pdv = str(result[2])
                cod_ticket = result[3]
                total = result[4]
                cod_consumidor = str(result[5])
                forma_pagamento = str(result[6])
                ean = str(result[7])
                descritivo_item = str(result[8])
                quantidade = result[9]
                valor = result[10]
                desconto_acrescimo_item = result[11]
                desconto_acrescimo_ticket = result[12]

                fwrite.writerow([data_transacao, cod_loja, cod_pdv, cod_ticket, total, cod_consumidor, forma_pagamento, 
                ean, descritivo_item, quantidade, valor, desconto_acrescimo_item, desconto_acrescimo_ticket])

            