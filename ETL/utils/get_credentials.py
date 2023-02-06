import json
from pathlib import Path
from os.path import expanduser
import os


def get_credentials(file, credentials_path=f'../../../../napp_pdi/napp_pdi'): # ADAPTADO PARA A AZURE
    # (file, credentials_path=f'{expanduser("~")}/.credentials/'): -> MODELO IDEAL
    """
    Informar o nome do arquivo de credencial que desejar
    file[string] --> Nome do arquivo junto do tipo de arquivo.
    """
    # print(Path(credentials_path).resolve())
    # credentials = Path(credentials_path).resolve()

    # if not credentials.exists():
    #     raise Exception("Credentials path not found, check if you have or in another directory")

    # file_credentials = credentials / file

    cred_json = {
            "username": "admin@nappsolutions.com",
            "password": "12345"
        } # PROVISÓRIO

    # if not file_credentials.exists():
    #     raise Exception("Credentials file not found, check if you have or in another directory")

    # file_json = json.loads(file_credentials.open().read())
    # return file_json
    return cred_json