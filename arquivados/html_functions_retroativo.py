import pandas as pd
import os
import time
import shutil
import dotenv

dotenv.load_dotenv()

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

pause_time = 10

# Set up the Chrome WebDriver (make sure to provide the correct path to chromedriver.exe)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(
    "--start-maximized"
)  # Ensures that the window opens in full screen


def rename_last_downloaded_file(download_path, dir_path, new_filename):
    files = os.listdir(download_path)
    files.sort(
        key=lambda x: os.path.getctime(os.path.join(download_path, x)),
        reverse=True,
    )
    if files:
        last_downloaded_file = files[0]
        time.sleep(4)
        shutil.move(
            rf"{download_path}\{last_downloaded_file}",
            rf"{dir_path}\{new_filename}",
        )


# %% ECCOSYS


def download_eccosys(
    datatxt, dataname, lista_clientes, download_path, final_path
):
    ### CREATE FOLDER IF IT DOESNT EXIST
    if not os.path.exists(final_path):
        os.makedirs(final_path)
        print(f"The folder {final_path} was created.")

    #### Driver está na pasta C:\Program Files (x86)\Google\driver\
    # Set up the Chrome WebDriver (make sure to provide the correct path to chromedriver.exe)
    driver = webdriver.Chrome(options=chrome_options)

    # Wait for some time to allow the page to load (you may need to adjust the sleep duration)
    driver.implicitly_wait(pause_time)
    wait = WebDriverWait(driver, pause_time)  # 10 seconds timeout

    for cliente in lista_clientes:
        df = pd.read_csv(
            rf"{os.getenv('path_mage')}mage_performance\Txt\{cliente}_eccosys.txt",
            delimiter="\t",
        )

        url = df["info"][0]
        username = df["info"][1]
        password = df["info"][2]

        driver.get(url)

        time.sleep(1)
        # Find the username and password input fields using XPath
        username_xpath = (
            "/html/body/div/div/div/div[1]/div[2]/div/form/div[2]/div/input"
        )
        password_xpath = (
            "/html/body/div/div/div/div[1]/div[2]/div/form/div[3]/div/input"
        )

        filename_prod = f"{cliente}_Produtos_{dataname}.xls"
        filename_vend = f"{cliente}_Vendas_{dataname}.csv"
        filename_esto = f"{cliente}_Estoque_{dataname}.xls"

        driver.find_element(By.XPATH, username_xpath).send_keys(username)
        driver.find_element(By.XPATH, password_xpath).send_keys(password)

        # Find the button using XPath
        button_xpath = (
            "/html/body/div/div/div/div[1]/div[2]/div/form/div[6]/div/input"
        )

        # Click the button
        driver.find_element(By.XPATH, button_xpath).click()

        ############### PRODUTOS ###############

        # time.sleep(pause_time + 2)
        # button_text = "Cadastros"
        # cadastros = wait.until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, f"//*[text()='{button_text}']")
        #     )
        # )
        # cadastros.click()

        # time.sleep(1)
        # button_text = "Produtos"
        # cadastros = wait.until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, f"//*[text()='{button_text}']")
        #     )
        # )
        # cadastros.click()

        # time.sleep(pause_time + 5)
        # filtro_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[3]/a"
        # wait.until(
        #     EC.element_to_be_clickable((By.XPATH, filtro_xpath))
        # ).click()

        # ativos_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[4]/div/div[1]/div[1]/ul/li[4]/a"
        # driver.find_element(By.XPATH, ativos_xpath).click()

        # time.sleep(pause_time)
        # acoes_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[3]/div/a[2]"
        # driver.find_element(By.XPATH, acoes_xpath).click()

        # time.sleep(pause_time)
        # button_text = "Exportar dados para planilha"
        # download1 = wait.until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, f"//*[text()='{button_text}']")
        #     )
        # )
        # download1.click()

        # # Find the dropdown using XPath
        # dropdown_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/div[2]/div/select"
        # )

        # # Initialize the Select object
        # dropdown = Select(driver.find_element(By.XPATH, dropdown_xpath))

        # # Exceção de select caso cliente = una, todos os produtos
        # if (
        #     cliente == "una"
        #     or cliente == "basicler"
        #     or cliente == "haut"
        #     or cliente == "morina"
        #     or cliente == "rery"
        #     or cliente == "uniquechic"
        # ):
        #     option_index_to_select = 0
        #     dropdown.select_by_index(option_index_to_select)

        # else:
        #     # Select the 2nd option (index starts from 0), produto pai
        #     option_index_to_select = 1
        #     dropdown.select_by_index(option_index_to_select)

        # # Find the checkbox using XPath
        # checkbox_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[1]/input"
        # )

        # # Click the checkbox to select it and unselect it, para marcar e desmarcar tudo
        # for i in range(2):
        #     driver.find_element(By.XPATH, checkbox_xpath).click()
        #     time.sleep(0.5)

        # sku_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[3]/input"
        # )
        # driver.find_element(By.XPATH, sku_xpath).click()

        # preco_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[10]/input"
        # )
        # driver.find_element(By.XPATH, preco_xpath).click()

        # preco_custo_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[11]/input"
        # )
        # driver.find_element(By.XPATH, preco_custo_xpath).click()

        # preco_ant_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[46]/input"
        # )
        # driver.find_element(By.XPATH, preco_ant_xpath).click()

        # time.sleep(8)
        # download_xpath = (
        #     "/html/body/div[5]/div/div[2]/div[2]/div/form/div[4]/input"
        # )
        # driver.find_element(By.XPATH, download_xpath).click()

        # time.sleep(pause_time + 60)
        # # time.sleep(pause_time)  # FOR MAINTENANCE ONLY

        # rename_last_downloaded_file(download_path, final_path, filename_prod)

        ############### VENDAS ###############

        time.sleep(pause_time - 5)
        button_text = "Vendas"
        vendas = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        vendas.click()

        time.sleep(1)
        button_text = "Pedidos de Venda"
        pedidos = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        pedidos.click()

        time.sleep(pause_time - 2)
        filtro2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[3]/a"
        driver.find_element(By.XPATH, filtro2_xpath).click()

        time.sleep(pause_time - 7)
        dia_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[4]/div/div/div[1]/ul/li[3]/a"

        driver.find_element(By.XPATH, dia_xpath).click()

        time.sleep(pause_time)
        data_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[4]/div/div/div[2]/div/div[2]/div/input"
        driver.find_element(By.XPATH, data_xpath).send_keys(Keys.CONTROL, "a")
        driver.find_element(By.XPATH, data_xpath).send_keys(Keys.DELETE)
        driver.find_element(By.XPATH, data_xpath).send_keys(datatxt)
        time.sleep(0.5)
        driver.find_element(By.XPATH, data_xpath).send_keys(Keys.ENTER)

        time.sleep(pause_time)
        # Find the dropdown using XPath
        dropdown2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[4]/div/div/div[3]/div[1]/select"
        # Initialize the Select object (selecionar situação > todos)
        dropdown2 = Select(driver.find_element(By.XPATH, dropdown2_xpath))
        option_index_to_select = 0
        dropdown2.select_by_index(option_index_to_select)

        time.sleep(pause_time)
        acoes2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[3]/div/a[2]"
        driver.find_element(By.XPATH, acoes2_xpath).click()

        time.sleep(1)
        button_text = "Exportar dados para planilha"
        download2 = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        download2.click()

        time.sleep(pause_time + 2)
        rename_last_downloaded_file(download_path, final_path, filename_vend)

        ############### ESTOQUE ###############

        # time.sleep(pause_time - 5)
        # button_text = "Relatórios"
        # relatorios = wait.until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, f"//*[text()='{button_text}']")
        #     )
        # )
        # relatorios.click()

        # time.sleep(1)
        # button_text = "Visão Financeira do Estoque"
        # relatorio_fin = wait.until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, f"//*[text()='{button_text}']")
        #     )
        # )
        # relatorio_fin.click()

        # time.sleep(1)
        # data2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[3]/div/div[2]/div[1]/div[1]/div[1]/div[1]/div/div/div/input"
        # driver.find_element(By.XPATH, data2_xpath).send_keys(Keys.CONTROL, "a")
        # driver.find_element(By.XPATH, data2_xpath).send_keys(Keys.DELETE)
        # driver.find_element(By.XPATH, data2_xpath).send_keys(datatxt)
        # time.sleep(0.5)

        # visualizar_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[3]/div/div[2]/div[1]/div[2]/div[1]/input[1]"
        # driver.find_element(By.XPATH, visualizar_xpath).click()

        # time.sleep(1)
        # download3_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[3]/div/div[2]/div[1]/div[2]/div[1]/input[2]"
        # driver.find_element(By.XPATH, download3_xpath).click()

        # time.sleep(pause_time)
        # rename_last_downloaded_file(download_path, final_path, filename_esto)

    driver.quit()
