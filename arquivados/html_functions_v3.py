### htlm_funtions replacing time.sleep

import pandas as pd
import os
import shutil
import dotenv

dotenv.load_dotenv()

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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

    # Define WebDriverWait
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

        wait.until(
            EC.presence_of_element_located((By.XPATH, username_xpath))
        ).send_keys(username)
        wait.until(
            EC.presence_of_element_located((By.XPATH, password_xpath))
        ).send_keys(password)

        # Find the button using XPath
        button_xpath = (
            "/html/body/div/div/div/div[1]/div[2]/div/form/div[6]/div/input"
        )

        # Click the button
        wait.until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        ).click()

        ############### PRODUTOS ###############

        button_text = "Cadastros"
        cadastros = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        cadastros.click()

        button_text = "Produtos"
        cadastros = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        cadastros.click()

        filtro_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[3]/a"
        element = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, filtro_xpath))
        )
        element.click()

        ativos_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[4]/div/div[1]/div[1]/ul/li[4]/a"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, ativos_xpath))
        ).click()

        acoes_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[1]/div[5]/table/tbody/tr/td[1]/div/div[1]/div[3]/div/a[2]"
        wait.until(EC.element_to_be_clickable((By.XPATH, acoes_xpath))).click()

        button_text = "Exportar dados para planilha"
        download1 = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        download1.click()

        # Find the dropdown using XPath
        dropdown_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/div[2]/div/select"
        )

        # Initialize the Select object
        dropdown = Select(
            wait.until(
                EC.presence_of_element_located((By.XPATH, dropdown_xpath))
            )
        )

        # Exceção de select caso cliente = una, todos os produtos
        if cliente == "una":
            option_index_to_select = 0
            dropdown.select_by_index(option_index_to_select)
        else:
            # Select the 2nd option (index starts from 0), produto pai
            option_index_to_select = 1
            dropdown.select_by_index(option_index_to_select)

        # Find the checkbox using XPath
        checkbox_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[1]/input"
        )

        # Click the checkbox to select it and unselect it, para marcar e desmarcar tudo
        checkbox = wait.until(
            EC.element_to_be_clickable((By.XPATH, checkbox_xpath))
        )
        for i in range(2):
            checkbox.click()

        sku_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[3]/input"
        )
        wait.until(EC.element_to_be_clickable((By.XPATH, sku_xpath))).click()

        preco_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[10]/input"
        )
        wait.until(EC.element_to_be_clickable((By.XPATH, preco_xpath))).click()

        preco_custo_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[11]/input"
        )
        wait.until(
            EC.element_to_be_clickable((By.XPATH, preco_custo_xpath))
        ).click()

        preco_ant_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/ul/li[46]/input"
        )
        wait.until(
            EC.element_to_be_clickable((By.XPATH, preco_ant_xpath))
        ).click()

        download_xpath = (
            "/html/body/div[5]/div/div[2]/div[2]/div/form/div[4]/input"
        )
        wait.until(
            EC.element_to_be_clickable((By.XPATH, download_xpath))
        ).click()

        wait.until(EC.presence_of_element_located((By.XPATH, download_xpath)))
        rename_last_downloaded_file(download_path, final_path, filename_prod)

        ############### VENDAS ###############
        button_text = "Vendas"
        vendas = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        vendas.click()

        button_text = "Pedidos de Venda"
        pedidos = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        pedidos.click()

        # filtro2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[3]/a/i"
        filtro2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[7]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[3]/a"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, filtro2_xpath))
        ).click()

        dia_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[4]/div/div/div[1]/ul/li[3]/a"
        wait.until(EC.element_to_be_clickable((By.XPATH, dia_xpath))).click()

        data_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[2]/div[4]/div/div/div[2]/div/div[2]/div/input"
        data_field = wait.until(
            EC.element_to_be_clickable((By.XPATH, data_xpath))
        )
        data_field.send_keys(Keys.CONTROL, "a")
        data_field.send_keys(Keys.DELETE)
        data_field.send_keys(datatxt)
        data_field.send_keys(Keys.ENTER)

        # Find the dropdown using XPath
        dropdown2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[1]/div[2]/select"

        # Initialize the Select object
        dropdown2 = Select(
            wait.until(
                EC.presence_of_element_located((By.XPATH, dropdown2_xpath))
            )
        )

        # Select the 2nd option (index starts from 0), 50
        option_index_to_select = 1
        dropdown2.select_by_index(option_index_to_select)

        acao_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[4]/div/a[2]"
        wait.until(EC.element_to_be_clickable((By.XPATH, acao_xpath))).click()

        download2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[6]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[4]/div/ul/li[1]/a"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, download2_xpath))
        ).click()

        rename_last_downloaded_file(download_path, final_path, filename_vend)

        ############### ESTOQUE ###############
        button_text = "Estoque"
        estoque = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        estoque.click()

        button_text = "Posição de estoque"
        posicao = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        posicao.click()

        filtro3_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[5]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[3]/a"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, filtro3_xpath))
        ).click()

        ativos2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[5]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[4]/div/div[1]/div[1]/ul/li[4]/a"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, ativos2_xpath))
        ).click()

        acoes2_xpath = "/html/body/div[5]/div/div[2]/div[2]/div[5]/div[5]/table/tbody/tr/td[1]/div[2]/div[1]/div[3]/div/a[2]"
        wait.until(
            EC.element_to_be_clickable((By.XPATH, acoes2_xpath))
        ).click()

        button_text = "Exportar dados para planilha"
        download3 = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[text()='{button_text}']")
            )
        )
        download3.click()

        rename_last_downloaded_file(download_path, final_path, filename_esto)

    driver.quit()
