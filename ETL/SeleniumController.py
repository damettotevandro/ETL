import logging
import time
import sys

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup

from etl.selenium_tools.selenium_controller import SeleniumController
from .base_downloader import WebDownloader

logger = logging.getLogger(__name__)

class TrinksDownloader(WebDownloader):
    def __init__(self, *, login, password, store_name=None, estorno=False,data_pagamento_option=False, address="https://www.trinks.com/Login", **kwargs):
        self.login = login
        self.password = password
        self.store_name = store_name
        self.address = address
        self.estorno = estorno
        self.data_pagamento_option = data_pagamento_option
        super().__init__(**kwargs)

    def download_files(self):
        # Create selenium controller
        with SeleniumController(download_path=str(self.download_folder)) as sc:
            sc.driver.maximize_window()
            main_window = sc.driver.window_handles[0]

            try:
                # Login page
                sc.get(self.address)
                sc.send_keys('fEmail', self.login)
                sc.send_keys('fSenha', self.password)
                sc.click('//*[@id="login"]/div/div/form/div[3]/div[2]/button')
                time.sleep(5)
            except:
                # Login again
                sc.send_keys('Senha', self.password)
                sc.click('realizarLogin')
            try:
                sc.click('//*[@id="fecharPopupDePromocao"]')
            except:
                pass

            try:
                sc.click('//*[@id="ext-gen1009"]/div[1]/div/div/button')
            except:
                pass
            try:
                ActionChains(sc.driver).send_keys(Keys.ESCAPE).perform()
            except Exception as errror:
                logger.error(f'Error: {errror}')

            if self.store_name:
                try:
                    sc.click('linkBackoffice')
                    time.sleep(1)
                    ActionChains(sc.driver).send_keys(Keys.TAB).perform()

                    # Store_name for search on "Meus Estabelecimentos"
                    ActionChains(sc.driver).send_keys(self.store_name).perform()
                    time.sleep(5)

                    sc.click(f'//a[@title="{self.store_name}"]')
                    time.sleep(5)
                except:
                    # Verificação para caso o usuário só possuir uma unidade parsear por ela.
                    html = sc.driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    unidade = str(soup.find("a", {"id": "linkBackoffice"}).text).strip()
                    if(self.store_name != unidade):
                        logger.error("Não foi coletado as vendas, pois não foi inserido o nome correto da unidade.")
                        sys.exit()
            
            # Go to report page
            sc.get('https://www.trinks.com/BackOffice/Relatorios/Financeiro')

            # Select and set payment type
            sc.wait('TipoData', condition_action='click', timeout=15)
            sc.click('TipoData')
            ActionChains(sc.driver).send_keys(Keys.UP).send_keys(Keys.ENTER).perform()

            sc.send_keys('DataInicio', self.start_date.strftime('%d/%m/%Y'))
            sc.send_keys('DataFim', self.end_date.strftime('%d/%m/%Y'))
            if self.data_pagamento_option:
                select = Select(sc.driver.find_element_by_id('TipoData'))
                select.select_by_visible_text('Data de Pagamento/Estorno')
            if self.estorno:
                sc.click('ExibirEstornos')

            time.sleep(25)

            sc.wait('exportar_relatoriofinanceiro',
                    condition_action='click', timeout=15)
            sc.click('exportar_relatoriofinanceiro')

            time.sleep(25)
            logger.debug('Download Complete')
