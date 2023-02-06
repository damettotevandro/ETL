from etl.remote_database import RemoteDataBase
from etl.utils import (
    normalize_monetary_value,
    normalize_date,
    date_to_datetime,
)


class SetaDataBase(RemoteDataBase):
    def __init__(self, cnpj, total=None, **kwargs):
        self.cnpj = cnpj
        self.total = total
        super().__init__(**kwargs)

    def process_data(self):
        # create connection to postgres
        database = self.connect_postgres()
        # connection postgres
        database.connect()

        days = (date_to_datetime(self.end_date) - date_to_datetime(self.start_date)).days

        query = """
        SELECT
            CONCAT(id_venda,'-',to_char(data_hora, 'YYYYMMDD')) AS codVenda,
            CASE cancelado WHEN 'S' THEN 0 ELSE subtotal - abs(desconto_acrescimo) END AS Total,
            data_hora,
            forma_pagamento,
            total_pago
        FROM
            auxiliar.VNAPVENDAS
        WHERE
            data_hora::date >= NOW()::date - interval '{} day'
        AND
            loja_filial = '{}'
        """.format(days, self.cnpj)

        # set query to cursor
        database.cursor.execute(query)
        results = database.cursor.fetchall()

        for result in results:
            ticket = result[0]
            value = float(normalize_monetary_value(result[1]))
            if self.total:
                value = float(normalize_monetary_value(result[4]))
            date_time = normalize_date(result[2])
            prefix = 'v'
            if value < 0:
                prefix = 'd'
            code = f'{prefix}-{ticket}'
            self.add_order(code, value, date_time)

        database.disconnect()
