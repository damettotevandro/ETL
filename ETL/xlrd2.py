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


class CEAParser(FileParser):
    def __init__(self, store_code,bruto_com_icms=False,first_date='',no_taxes=False, **kwargs):
        self.store_code = store_code
        self.bruto_com_icms = bruto_com_icms
        self.first_date = first_date
        self.no_taxes = no_taxes
        super().__init__(**kwargs)

    def process_data(self):
        def normalize_value(self):
            print(self)
            if '-' in str(self):
                valr_formated = -1 * float(normalize_monetary_value(str(self).replace('-', '')))
            else:
                valr_formated = float(normalize_monetary_value(str(self)))
            return valr_formated
            
        for file in Path(self.input_folder).iterdir():
            if self.store_code in file.stem:
                logger.debug(f'Reading file: {file}')
                if not 'xlsx' in file.suffix.lower():
                    logger.warning(f"extension error: {file} is not a valid xlsx file")
                    continue
                book = xlrd.open_workbook(str(file.absolute()), encoding_override="utf-8")
                for sheet in book.sheets():
                    for rx in range(1,sheet.nrows):
                        row = sheet.row(rx)
                        if len(row[0].value) != 10:
                            continue
                        date = normalize_date(row[0].value, formats='%d.%m.%Y')
                        total = normalize_value(row[1].value)
                        icms = normalize_value(row[2].value)
                        cofins = normalize_value(row[3].value)
                        pis = normalize_value(row[4].value)
                        vl_imp = -float(icms + cofins + pis)
                        code = f"{date:%d%m%Y}"
                        code2 = f"IMP-{date:%d%m%Y}"
                        if self.first_date:
                            if normalize_date(self.first_date) > normalize_date(date):
                                continue
                        if self.bruto_com_icms:
                            code2 = f'ICMS-{date:%d%m%Y}'
                            vl_imp = icms
                        self.add_order(code, total, date, file_name=file.stem)
                        if not self.no_taxes:
                            self.add_order(code2, vl_imp, date, file_name=file.stem)