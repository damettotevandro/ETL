import requests
from bs4 import BeautifulSoup

from napp_pdi.utils import normalize_date, normalize_monetary_value


def convert_currency(from_code, to_code, value, date):
    if isinstance(date, str):
        date = normalize_date(date)

    value = float(normalize_monetary_value(value))

    r = requests.post(f'https://www.xe.com/pt/currencytables/?from={from_code.upper()}&date={date:%Y-%m-%d}')
    soup = BeautifulSoup(r.text.encode('utf-8'), 'html.parser')
    table = soup.find('table', id='historicalRateTbl')
    tbody = table.find('tbody')
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if not tds:
            continue
        code = tds[0].text
        if code.upper() == to_code.upper():
            value_currency = float(tds[2].text.replace(',', '.'))
            total = value * value_currency
            total = normalize_monetary_value(f'{total}'.replace('.', ','), input_sep=',')
            return total
