import logging
import csv
from pathlib import Path

from .base import FileParser
from etl.utils import (
    normalize_monetary_value,
    normalize_date,
    unzip_file,
)

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class ZeusManagerParser(FileParser):
    def __init__(self,loja=None, **kwargs):
        self.loja = loja
        super().__init__(**kwargs)

    def process_data(self):
        for file in Path(self.input_folder).glob("*.zip"):
            try:
                unzip_file(str(file.absolute()))
            except:
                logger.warning(f'file {file.stem} is empty')
        for file in Path(self.input_folder).glob("*.csv"):
            logger.debug(f'Reading file: {file}')
            with open(str(file.absolute()), 'r') as csv_file:
                freader = csv.reader(csv_file, delimiter=';')
                for row in freader:
                    if not row or not row[0].isdigit():
                        continue
                    loja = row[0]
                    if self.loja:
                        if self.loja != loja:
                            continue
                    id = row[1]
                    coo = row[2]
                    dt = row[3].split('-03:')[0]
                    date = dt.replace('T', ' ')
                    datezero = date.split()
                    if datezero[1].startswith('9'):
                        setDatetime = normalize_date(datezero[0])
                    else:
                        setDatetime = normalize_date(date)
                    valorTotal = normalize_monetary_value(row[6])
                    pedidoCode = f"{loja}-{coo}-{setDatetime:%Y%m%d}"
                    self.add_order(pedidoCode, valorTotal, setDatetime)
