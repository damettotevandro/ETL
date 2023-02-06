import csv
import logging
from pathlib import Path

from .base import FileParser
from etl.utils import (
    normalize_monetary_value,
    normalize_date,
)

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class AFormulaParser(FileParser):
    def __init__(self, store_code, **kwargs):
        self.store_code = store_code
        super().__init__(**kwargs)

    def process_data(self):

        for file in Path(self.input_folder).iterdir():
            logger.debug(f'Reading file: {file}')
            if not 'txt' in file.suffix.lower():
                logger.warning(f"extension error: {file} is not a valid file txt")
                continue
            with open(str(file.absolute()), 'r') as csv_file:
                freader = csv.reader(csv_file, delimiter=';')
                for row in freader:
                    if not row or not row[0].isdigit():
                        continue
                    filial = row[0]
                    if self.store_code != filial:
                        continue
                    id_venda = row[1]
                    date = row[2].split('.')[0]
                    data_formated = normalize_date(date)
                    valor = normalize_monetary_value(row[3])
                    pedidoCode = f'{filial}-{id_venda}-{data_formated:%Y%m%d}'
                    valorTotal = valor
                    setDatetime = f'{data_formated:%Y-%m-%d %H:%M:%S}'
                    self.add_order(pedidoCode, valorTotal, setDatetime, file_name=file.stem)