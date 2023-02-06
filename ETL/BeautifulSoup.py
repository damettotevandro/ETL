from elt.base import PDIBase
import requests
from bs4 import BeautifulSoup
import logging

from elt.utils import(
    normalize_date,
    normalize_monetary_value,
    list_date
)

logger = logging.getLogger(__name__)

class BemPratico(PDIBase):
    def __init__(self, pin, report_code, login=None, password=None, skip_first_login=False, login_token = False, rel_detalhamento_vendas=False, **kwargs):
        self.login = login
        self.password = password
        self.pin = pin
        self.report_code = report_code
        self.skip_first_login = skip_first_login
        self.login_token = login_token
        self.rel_detalhamento_vendas = rel_detalhamento_vendas
        super().__init__(**kwargs)
        
    def process_data(self):
        s = requests.Session()
        headers = {
            'Origin': 'https://app3.bempratico.com.br',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://app3.bempratico.com.br/',
        }
        
        data = {
            'last_scroll': '',
            'login': self.login,
            'senha': self.password,
            'submitted': '1',
            'do_login': '1',
            'forgot_password': ''
        }

        if not self.skip_first_login:
            #login 1
            response = s.post('https://app3.bempratico.com.br/', headers=headers, data=data)
            #get url_code
            html = str(response.content)
            login_cod = html.split("https://app3.bempratico.com.br?p2=&p=")[1].split('&')[0]
        else:
            first_req = f'https://app3.bempratico.com.br//?p={self.report_code}&s=login_master&token={self.login_token}'
            request = s.get(first_req, headers=headers)
            login_cod = self.report_code

        #login 2
        url_login2 = f'https://app3.bempratico.com.br/?p={login_cod}&s=login2'
        
        headers['Referer'] = url_login2
        
        data = {
            'last_scroll': '',
            'change_toggle4': '',
            'nf_value': self.pin,
            'avancar': '',
            'contato': '',
        }

        response_login2 = s.post(url_login2, headers=headers, data=data)
        
        headers = {
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
            'Sec-Fetch-Dest': 'document',
        }

        days = list_date(start_date=self.start_date, end_date=self.end_date)
        for day in days:
            if self.rel_detalhamento_vendas:
                #Get sales Vendas > Detalhamento de venda
                params = {
                    'p': self.report_code,
                    's': '10',
                    'data': f'{day:%Y-%m-%d}',
                }

                response = s.get('https://app3.bempratico.com.br//', headers=headers, params=params)
                soup = BeautifulSoup(response.content, 'html.parser')
                div_table = soup.find('div', {'class':'table_classe_forms_container'})
                table_id = div_table.find('table', {'id':'classe_forms'})
                if not table_id:
                    continue
                tbody = table_id.find("tbody")
                trs = tbody.find_all("tr")
                for tr in trs:
                    tds = tr.find_all("td")
                    if len(tds) == 0:
                        continue
                    divs = tds[0].find_all("div")
                    if len(divs) == 0:
                        continue
                    cod_venda = str(divs[0].text).split("Venda")[1].strip()
                    valor = normalize_monetary_value(divs[6].text)
                    data = normalize_date(divs[9].text)
                    if "Pedido Cancelado" in str(tr):
                        valor = 0.0
                    code = f'{cod_venda}-{data:%Y%m%d}'
                    self.add_order(code, valor, data)
            else:
                #Get sales Vendas > Extrato de pedidos
                params = {
                    'p': self.report_code,
                    's': 'report_center_vendas_extrato',
                    'data_selected': f'{day:%Y-%m-%d}',
                }

                response = s.get('https://app3.bempratico.com.br/', headers=headers, params=params)

                soup = BeautifulSoup(response.content, 'html.parser')
                div_sales = soup.find('div', {'class':'Report_box'})
                div_rows = div_sales.find_all('div', {'id':'linha_'})

                if len(div_rows) == 0:
                    logger.warn(f'Nao consta vendas dia {day:%Y-%m-%d}')
                    continue

                for row in div_rows:
                    columns_div = row.find_all('div')
                    if len(columns_div) < 11:
                        continue
                    cod_data = str(columns_div[0].text).split(' ') 
                    hora = cod_data[1]
                    cod = int(cod_data[0].find('/')) - 2
                    cod = str(cod_data[0])[0:cod].strip()
                    total = normalize_monetary_value(columns_div[10].text)
                    formated_date = normalize_date(f'{day:%Y-%m-%d} {hora}')
                    code = f'{cod}-{day:%Y%m%d}'
                    self.add_order(code, total, formated_date)
