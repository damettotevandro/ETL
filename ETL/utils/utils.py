import csv
import datetime
import re
import json
import logging
import pandas as pd
import typing as T
import shutil
import operator
import time
from zipfile import ZipFile
from collections.abc import Mapping, Sequence
from numbers import Number
from pathlib import Path

from bs4 import BeautifulSoup

from napp_pdi.tables import TableIterator
from .compact_tools import zip_files

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


def normalize_str(
    text: T.Union[str, None],
    translate_spaces_to: str = '\x20',
    normalize_case: bool = True,
    white_list: str = '',
) -> str:
    """Normaliza strings, removendo pontuação e acentuação e arrumando espaçamento

    Usar para comparar títulos de colunas em tabelas, e outras entradas
    que podem ter diferenças de digitação.
    White-list: string com caracteres de pontuação que devem ser mantidos.
    (Não funciona para caracteres acentuados)
    """
    import unicodedata
    import string

    valid_chars = string.ascii_lowercase + string.digits + " " + white_list

    if not text:
        return ""

    # Removes extra spaces:
    text = text.strip()

    if normalize_case:
        text = text.lower()

    # removes accented characters
    text = unicodedata.normalize("NFKD", text).encode("ASCII", errors="ignore").decode("ASCII")

    # Convert all white-space chars (\n\t,\x20 + strange unicode spacings) to space (\x20)
    text = re.sub("\s", "\x20", text, flags=re.MULTILINE)

    # remove non-white-listed chars:
    text = "".join(char for char in text if char in valid_chars)

    # coaslesce all space sequences to a single space and translate space character:
    text = re.sub("\s+", translate_spaces_to, text)

    return text


def normalize_monetary_value(
    text: str,
    output_sep: str = ".",
    input_sep: T.Optional[str] = None,
    monetary_prefix: T.Optional[str] = None,
) -> str:
    r"""Recebe qualquer string com um valor numérico,
    e devolve na forma "nnnn.mm".
    Args:
      - text: String contendo valor a extrair
      - output_sep: Separador decimal para a saída (padrão: ".")
      - input_sep: Separador decimal usado nos dados de entrada.
                    Por padrão a função tenta detectar o separador entre "." e ",",
                    mas nos casos em que isso geraria ambiguidade, levanta um ValueError
      - monetary_prefix: Expressão regular com o prefixo monetário opcional para o valor.
            Por padrão: '([Rr]?\\$|BRL|brl)' que casa com r$, R$, $, brl ou BRL
            (O prefixo sempre é opcional, e olhado no começo da string)
    Exemplos:
    'R$ 140,00' -> '140.00'
    '$140.00' -> '140.00'
    ' 140,0' -> '140.00'
    '140.'' -> '140.00'
    '140' -> '140.00'
    '145.343,23' -> '145323.00'
    '140.0000',
    '140,000' e
    '140.234' -> ValueError: Valor ambiguo passe o parametro "input_sep" para desambiguar
    '140,234,23' -> ValueError: mais de um separador
    '140,234.23' -> "1401234.23" (veja os arquivos de testes para mais casos)

    """
    if isinstance(text, Number):
        return f"{text:.02f}"

    if not text:
        text = "0"
    text = text.strip()

    # find input decimal separator:
    if not input_sep:
        input_sep_expr = r'[\.,]'

    else:
        input_sep_expr = re.escape(input_sep)
    # Busca separador decimal nas três últimas posições do texto:
    cents_expr = fr"({input_sep_expr})(\d{{0,2}})"
    cents_match = re.search(cents_expr, text[-3:])

    if cents_match:
        # Garante duas casas depois da vírgula
        # Operados > na linha abaixo estava colocando um 0 antes do digito
        # Em cados com 1 casa decimal 2173.2 estava transformando em 2173.02
        # Inverti para < para que fique 2173.20 certo.
        cents = f"""{cents_match.groups()[1]:<02s}"""
        found_sep = cents_match.groups()[0]
        try:
            integer_part, _ = text.split(found_sep)
        except ValueError:
            raise ValueError(f"Mais de um separador decimal no valor: {text}")

    elif not input_sep:
        # Se não foi passado um separador decimal explícito,
        # e houver um "." ou "," fora do lugar, ValueError:
        if re.search(input_sep_expr, text):
            raise ValueError(f"Valor monetário ambíguo: {text}. Passe o separador explicitamente")
        integer_part = text
        cents = "00"
    else:
        # Mais casas decimais do que o esperado, mas o separador é explícito
        separadores = text.count(input_sep)
        if separadores > 1:
            raise ValueError(f"Valor monetário mal formatado: {text}")
        elif separadores == 0:
            integer_part = text
            cents = "00"
        else:
            integer_part, cents_part = text.split(input_sep)
            # Arredondar para apenas duas casas de centavos:
            cents_int = int(cents_part[0:2]) + int(int(cents_part[2]) >= 5)
            cents = f"{cents_int:02d}"

    if not monetary_prefix:
        # Prefixo default bate com r$, R$, $, brl, BRL
        monetary_prefix = r'([Rr]?\$|BRL|brl)'
    prefix_found = re.match(monetary_prefix, integer_part)
    if prefix_found:
        integer_part = integer_part[prefix_found.end():]

    digits = ""
    signal = ""
    for char in integer_part:
        if char in "0123456789":
            digits += char
        elif char in "-+":
            if digits or signal:
                raise ValueError(f"Valor monetário mal formatado: {text}")
            signal = char
        elif char == " ":
            if digits:
                raise ValueError(f"Valor monetário mal formatado: {text}")
        elif char not in "+-,.":
            raise ValueError(f"Valor monetário mal formatado: {text}")

    if signal == '+':
        signal = ''

    if not digits:
        digits = "0"

    value = f"{signal}{digits}{output_sep}{cents}"
    if value == "-0.00":
        value = "0.00"
    return value


def normalize_date(text, formats=None):
    """TODO: possibilitar o reconhecimento
    dos tipos de dados mais usados, e parametrizar
    casos ambiguos
    Args:
      - text: String contendo a data.
      - formats: uma string ou lista de strings contendo o formato da data.
    '01/09/2019' -> '2019-09-01 00:00:00'
    '01-09-2019' -> '2019-09-01 00:00:00'
    '2019/09/01' -> '2019-09-01 00:00:00'
    '2019-09-01' -> '2019-09-01 00:00:00'
    '01/09/19' -> '2019-09-01 00:00:00'
    '01/09/2019 10:50' -> '2019-09-01 10:50:00'
    """
    formats_date = [
        '%d/%m/%Y', '%d/%m/%y', '%d/%m/%Y %H', '%d/%m/%y %H', '%d/%m/%Y %H:%M', '%d/%m/%y %H:%M',
        '%d/%m/%Y %H:%M:%S', '%d/%m/%y %H:%M:%S', '%d/%m/%Y %H:%M:%S.%f', '%d/%m/%y %H:%M:%S.%f',
        '%Y/%m/%d', '%Y/%m/%d %H', '%y/%m/%d %H', '%Y/%m/%d %H:%M', '%y/%m/%d %H:%M',
        '%Y/%m/%d %H:%M:%S', '%y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M:%S.%f', '%y/%m/%d %H:%M:%S.%f',
        '%d-%m-%Y', '%d-%m-%y', '%d-%m-%Y %H', '%d-%m-%y %H', '%d-%m-%Y %H:%M', '%d-%m-%y %H:%M',
        '%d-%m-%Y %H:%M:%S', '%d-%m-%y %H:%M:%S', '%d-%m-%Y %H:%M:%S.%f', '%d-%m-%y %H:%M:%S.%f',
        '%Y-%m-%d', '%Y-%m-%d %H', '%y-%m-%d %H', '%Y-%m-%d %H:%M', '%y-%m-%d %H:%M',
        '%Y-%m-%d %H:%M:%S', '%y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S', '%d-%m-%YT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f','%d-%b-%Y.%H:%M:%S', 
        '%Y-%m-%d%H:%M:%S','%Y%m%d%H%M%S','%Y-%m-%d%H%M%S', '%Y%m%d %H:%M:%S', '%Y%m%d %H:%M',
        '%Y-%m-%dT%H:%M:%S.%fZ'
    ]

    if formats:
        if isinstance(formats, str):
            formats_date = [formats]
        else:
            formats_date = formats

    if isinstance(text, datetime.datetime):
        return text

    text = text.strip()

    results = []

    for form in formats_date:
        try:
            results.append(datetime.datetime.strptime(text, form))
        except ValueError:
            continue

    if results:
        if len(results) > 1:
            logger.warning(f'este é um caso abíguo: "{text}"" - {results}. Verificar Formato correto.')
            raise ValueError()
        result = results[0]
    else:
        logger.warning(f'Não foi possível converter a data "{text}", verificar o se é uma data e formato válido')
        raise ValueError()

    return result


def normalize_time_ranges(
    days: T.Optional[int] = None,
    start_date: T.Optional[T.Union[datetime.datetime, str]]=None,
    end_date: T.Optional[T.Union[datetime.datetime, str]]=None
) -> T.Tuple[datetime.datetime, datetime.datetime]:
    """Devolve uma data de inicio e data de fim
    corretos dados opcionalmente qualquer combinação sensata
    de data de fim, quantidade de dias ou data final.
    Se a "end_date" não for passada, a data de atual
    é assumida, mas se for passada "start_date" e "days",
    "end_date" é calculada como start_date + days
    """
    if isinstance(start_date, str):
        start_date = normalize_date(start_date)

    if isinstance(end_date, str):
        end_date = normalize_date(end_date)

    if not end_date:
        if days and start_date:
            end_date = start_date + datetime.timedelta(days=days)
        else:

            end_date = datetime.date.today()
            # Se hoje é dia 1, então define a data final até ontem
            if end_date.day == 1:
                end_date = end_date - datetime.timedelta(days=1)

    if days:
        # Subtrai data final por "dias"
        if not start_date:
            start_date = end_date - datetime.timedelta(days=days)
        elif (end_date - start_date).days != days:
            raise ValueError("Parâmetros inconsistentes - número de dias não bate com datas passadas")
    if not start_date and end_date:
        raise ValueError("Pelo menos o número de dias tem que ser passado")

    return start_date, end_date


def date_to_datetime(date):
    return datetime.datetime.fromordinal(date.toordinal())


def list_date(
    start_date: T.Optional[T.Union[datetime.datetime, str]]=None,
    end_date: T.Optional[T.Union[datetime.datetime, str]]=None
) -> T.Tuple[datetime.datetime, datetime.datetime]:
    """Necessário informar start_date e end_date e vai retornar uma lista
    com todas as datas dentro desse range de datas.
    """
    if isinstance(start_date, str):
        start_date = normalize_date(start_date)

    if isinstance(end_date, str):
        end_date = normalize_date(end_date)

    list_date_range = []
    if not start_date or not end_date:
        logger.error('Informe start_date e end_date para retornar a lista')
        raise Exception
    try:
        for n in range(int((end_date - start_date).days) + 1):
            list_date_range.append(start_date + datetime.timedelta(n))
    except Exception as errror:
        logger.error(f'ocorreu um erro ao gerar a lista de datas: {errror}')
        raise
    return list_date_range


def normalize_path(path):
    if str(path).startswith("file://"):
        path = path[len("file://"):]
    return path


def generate_output(data, output_file, headers=None):
    with open(output_file, 'wt', encoding="utf-8") as csvfile:
        fwrite = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if headers:
            fwrite.writerow(headers)
        fwrite.writerows(data)
        logger.info(f'"{output_file}" successfully generated!')


def backup(folder='files'):
    now = datetime.datetime.now().strftime('%d%m%Y_%H%M%S')
    path = Path(folder)
    files = _get_files_backup(path)
    path_backup = path / 'backup'

    if not path_backup.exists():
        path_backup.mkdir()
    else:
        if not path_backup.is_dir():
            path_backup.rename(f'{path_backup}_file')
            path_backup.mkdir()

    for file in path.iterdir():
        if 'backup' in str(file.absolute()):
            continue
        files.append(file)

    zip_files(files, path_backup, name_zip=f'backup_{now}.zip')


def _get_files_backup(path):
    path = Path(path)

    files = []

    for file in path.iterdir():
        if file.is_file():
            files.append(file)
        elif file.is_dir():
            files.extend(_get_files_backup(file))

    return files


def clear_folder(folder='files/'):
    folder = Path(folder).absolute()

    for arq in folder.iterdir():
        if arq.is_file():
            arq.unlink()
        elif arq.is_dir():
            if not 'backup' in str(arq.name).lower():
                clear_folder(arq)
            continue

    if not 'files' in str(folder.name).lower():
        folder.rmdir()


def group(data: list,
          params: list,
          columns: list,
          aggr: dict,
          index: list = None,) -> list:
    data_frame = pd.DataFrame(data, columns=columns, index=index)
    group_list = params.copy()

    if index:
        group_list.append(index)

    result = data_frame.groupby(group_list).agg(aggr)

    # result = result.astype('str')
    result_list = result.get_values().tolist()

    return result_list


def unique(data: list) -> list:
    data_frame = pd.DataFrame(data)
    data_list = data_frame.drop_duplicates().get_values().tolist()
    return data_list

new_csv_lines = set()
def add_column_csv(file, index1, index2, operator1, delimiter='|', index3=None, operator2=None, operator3=None, header=None, sep=None):
    csv_lines = []
    ops = { "+": operator.add, "-": operator.sub, "*": operator.mul, "/": operator.truediv }
    with open(str(file.absolute()), 'r', encoding='latin1') as csv_file:
        freader = csv.reader(csv_file, delimiter=delimiter)
        if header:
            next(freader)          
        for row in freader:            
            try:
                row[index1] = normalize_monetary_value(row[index1], input_sep=sep)
            except:
                print(f'valor invalido - {row[index1]}')
                continue             
            if len(row) <= 7:
                continue
            if row[index1].strip() == '' or row[index1] is None:
                row[index1] = 0
                
            if row[index2].strip() == '' or row[index2] is None:
                row[index2] = 0
            
            row[index2] = normalize_monetary_value(row[index2], input_sep=sep)
            if index3:
                row[index3] = normalize_monetary_value(row[index3], input_sep=sep)

            #se não tiver o operador2
            if not operator2:        
                new_column = ops[operator1](float(row[index1]),float(row[index2]))
                row.append(new_column)
                csv_lines.append(row) 
            #se tiver o operador2, é criado uma variavel operation1 para receber o resultado da primeira operacao
            #e usado para a operacao seguinte
            elif operator2 and row[index3] and not operator3:
                operation1 = ops[operator1](float(row[index1]),float(row[index2]))                        
                new_column = ops[operator2](operation1,float(row[index3]))                  
                row.append(new_column)
                csv_lines.append(row)
            #Utilizado caso o campo de desconto unitario não venha multiplicado pela quantidade        
            elif operator3:        
                ven = ops[operator1](float(row[index1]),float(row[index2]))
                des = ops[operator3](float(row[index1]),float(row[index3]))                      
                new_column = ops[operator2](ven,des)
                row.append(new_column)
                csv_lines.append(row)
            else:
                row.append(new_column)
                csv_lines.append(row)
                
        for line in csv_lines:
            new_csv_lines.add(tuple(line))
            
    with open(f'./files/new_file.csv', 'w', encoding='latin1') as new_csv_file:
        fwriter = csv.writer(new_csv_file, delimiter=delimiter)
        for item in new_csv_lines:
            fwriter.writerow(item)
        
    return new_csv_lines


def sum_unit_values(file, by, target, separator, columns, convert_columns=None, header=None, error_bad_lines=True, encoding='latin-1') -> list:
    csv_data = pd.read_csv(file,
                sep=separator,
                encoding=encoding,
                names=columns,
                index_col=False,
                header=header,
                error_bad_lines=error_bad_lines
            )
    df = pd.DataFrame(columns=columns, data = csv_data)

    if convert_columns:
        for column in convert_columns:
            df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('', 0).replace('None', 0)
            try:
                df = df.replace(',','.', regex=True).astype({column: float})
            except:
                df = df.astype({column: float})
    
    df_ag = df.groupby(by=[by])[target].sum()

    df_f = pd.merge(df,df_ag, left_on=by, right_index=True)

    return df_f.get_values().tolist()

def every_downloads_chrome(driver):
    if not driver.current_url.startswith("chrome://downloads"):
        driver.get("chrome://downloads/")
        try: 
            # confirmando saida do erp caso tenha
            driver.switch_to.alert.accept()
        except: 
            pass
    return driver.execute_script("""
        return document.querySelector('downloads-manager')
        .shadowRoot.querySelector('#downloadsList')
        .items.filter(e => e.state === 'COMPLETE')
        .map(e => e.filePath || e.file_path || e.fileUrl || e.file_url);
        """)

def sum_unit_values_excel(file, by, target, columns, convert_columns=None, header=None) -> list:
    excel_data = pd.read_excel(file,
                names=columns,
                index_col=False,
                header=header
            )
    df = pd.DataFrame(columns=columns, data = excel_data)
    if convert_columns:
        for column in convert_columns:
            df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('', 0).replace('None', 0)
            try:
                df = df.replace(',','.', regex=True).astype({column: float})
            except:
                df = df.astype({column: float})
    df_ag = df.groupby(by=[by])[target].sum()
    df_f = pd.merge(df,df_ag, left_on=by, right_index=True)
    return df_f.get_values().tolist()



def function_timeout(function, attempts = 60, sleep = 1, counter_action = lambda: logger.warn('Trying again...')):
    '''Repete a execução da função enquanto a mesma retorna alguma excessão.

    O número de tentativas é representado pelo 'attempts' (padrão 60);

    O tempo de espera entre tentativas é representado pelo 'sleep' (padrão 1);

    A ação que deve ser executada entre excessões é representada pelo counter_action (padrão logger.warn('Trying again...') );
    
    E em caso de execução bem sucedida retorna o valor retornado pela função caso tenha.

    Lança a ultima excessão ocorrida caso o número de tentativas se esgote.

    Ex:

    def funcao():
        print("função")
    
    function_timeout(funcao)

    OU

    lamb = lambda: print("lambda")

    function_timeout(lamb)
    '''
    error = ''
    for i in range(attempts):
        try:
            return function()
        except Exception as e:
            error = str(e)
            counter_action()
            time.sleep(sleep)
    raise Exception(error)
            
new_excel_lines = set()
def add_column_excel(file, index1, index2, operator1, index3=None, operator2=None, operator3=None, delimiter=';', header=None, sep=None):
    excel_lines = []
    ops = { "+": operator.add, "-": operator.sub, "*": operator.mul, "/": operator.truediv }
    excel_data = pd.read_excel(file, header=0)
    df = pd.DataFrame(data=excel_data)
    for row in df.get_values().tolist():            
        if len(row) <= 7:
            continue
        if row[index1] == '' or row[index1] is None:
            row[index1] = 0
            
        if row[index2] == '' or row[index2] is None:
            row[index2] = 0                  
        #se não tiver o operador2
        if not operator2:
            new_column = ops[operator1](row[index1],row[index2])
            row.append(new_column)
            excel_lines.append(row)
        #se tiver o operador2, é criado uma variavel result para receber o resultado da primeira operacao
        #e usado para a operacao seguinte
        if operator2 and index3 and not operator3:        
            result = 0                         
            new_column = result = ops[operator1](row[index1],row[index2]) + ops[operator2](result,row[index3])                  
            row.append(new_column)
            excel_lines.append(row)
        #Utilizado caso o campo de desconto unitario não venha multiplicado pela quantidade          
        elif operator3:        
            ven = ops[operator1](row[index1],row[index2])
            des = ops[operator3](row[index1],row[index3])                      
            new_column = ops[operator2](ven,des)
            row.append(new_column)
            excel_lines.append(row)
        else:
            row.append(new_column)
            excel_lines.append(row)
        
        for line in excel_lines:
            new_excel_lines.add(tuple(line))

    with open(f'./files/new_file.csv', 'w', encoding='latin1') as new_csv_file:
        fwriter = csv.writer(new_csv_file, delimiter=delimiter)
        for item in new_excel_lines:
            fwriter.writerow(item)
    return new_excel_lines