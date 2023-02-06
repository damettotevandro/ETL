import requests
from bs4 import BeautifulSoup
from datetime import timedelta
from napp_pdi.utils import normalize_date, normalize_monetary_value,normalize_time_ranges

def convert_currency_banco_central(from_code, to_code,days):
    start_date = normalize_time_ranges(days)[0]
    end_date = normalize_time_ranges(days)[1]

    print(start_date, ' - ', end_date)
    
    x = start_date
    # print(start_date.weekday())
    # print(start_date)
    #checking if is Saturday
    if start_date.weekday() == 5:
        start_date = start_date - timedelta(days=1)
    
    #checking if is sunday
    if start_date.weekday() == 6:
        start_date = start_date - timedelta(days=2)

    # print(start_date)
    row = dict()
    while(start_date<=end_date):
        r = requests.get(f'https://www3.bcb.gov.br/bc_moeda/rest/converter/1/1/{from_code}/{to_code}/{start_date}')
        soup = BeautifulSoup(r.text, 'lxml')
        try:
            value = soup.find('valor-convertido').get_text()
        except AttributeError:
            pass
        if x <= start_date:
            #returning dates than start_date
            row[start_date.strftime('%Y%m%d')] = value
        start_date = start_date + timedelta(days=1)

    return row
