import fnmatch
import logging
import xlrd
from pathlib import Path

from .base import FileParser
from etl.utils import (
    normalize_monetary_value,
    normalize_str,
    normalize_date,
)

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class AntixParser(FileParser):
    def __init__(self, filial, **kwargs):
        self.filial = filial
        super().__init__(**kwargs)

    def process_data(self):
        for file in Path(self.input_folder).iterdir():
            logger.info(f'Reading file: {file}')
            if not 'xlsx' in file.suffix.lower():
                logger.warn(f"extension error: {file} is not a valid file xlsx")
                continue          
            book = xlrd.open_workbook(str(file.absolute()), encoding_override="utf-8")
            for sheet in book.sheets():
                for rx in range(1,sheet.nrows):
                    row = sheet.row(rx)
                    if self.filial != row[21].value:
                        continue
                    operacao = row[0].value
                    dt = str(xlrd.xldate.xldate_as_datetime(row[1].value, book.datemode))
                    dtdois = dt.split(' ')[0].replace('-','').replace('-','')
                    total = normalize_monetary_value(row[6].value)
                    documento = row[5].value
                    nf = row[2].value

                    if nf == '':
                        codVenda = f'{documento}-{dtdois}'
                    else:
                        codVenda = f'{nf}-{documento}-{dtdois}'

                    if operacao == "E":
                        total = float(total) * -1
                    self.add_order(codVenda, total, dt)

 