import requests
import json
from bs4 import BeautifulSoup

conections = list()

server_sync = 'http://67.205.153.6:8000/'

def check_port(ip):
    
    page = requests.get(server_sync)
    soup = BeautifulSoup(page.text, 'html.parser')

    file = str(soup).replace('}','').split('{')

    for item in file:
        conections.append(item)

    for line in conections:
        if ip in line:
            print('Loja identificada no servidor do Sync')
            reg = line.split(',')
            for item in reg:
                if 'porta' in item:
                    porta = item.split(':')[1]
                    print(f'Porta: {porta}')
    return porta
