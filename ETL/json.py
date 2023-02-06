import json
import logging
from pathlib import Path

from .base import FileParser
from etl.utils import (
    normalize_monetary_value,
    normalize_date,
)

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class AranduParser(FileParser):
    def __init__(self, store_code, **kwargs):
        self.store_code = store_code
        super().__init__(**kwargs)

    def process_data(self):
        for file in Path(self.input_folder).iterdir():
            logger.debug(f'Reading file: {file}')
            if not 'json' in file.suffix.lower():
                logger.warning(f"extension error: {file} is not a valid json file")
                continue
            with open(str(file.absolute()), 'r') as fp:
                try:
                    content = json.load(fp)
                except ValueError as error:
                    logger.error(f'Erro: {error} em {file}')
                    continue
                for item in content:
                    cod_loja = item["cod_loja"]
                    if cod_loja == self.store_code:
                        cod_venda = item["cod_venda"]
                        total = float(normalize_monetary_value(item["total"]))
                        try:
                            dt = normalize_date(item["data_hora"])
                        except:
                            dt = normalize_date(item["data_hora"].split()[0])
                        status = item["cancelado"]
                        devolucao = item["troca"]
                        prefix = 'v'

                        if devolucao:
                            total *= -1
                            prefix = 'd'

                        if status:
                            total = 0.0

                        pedidoCode = f'{prefix}-{cod_loja}-{cod_venda}-{dt:%Y%m%d}'
                        valorTotal = total
                        setDatetime = dt

                        self.add_order(pedidoCode, valorTotal, setDatetime, file_name=file.stem)