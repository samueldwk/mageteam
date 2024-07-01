import pandas as pd
import datetime
import os
import time
import shutil
import date_functions as datef
import path_functions as pathf


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()
# #Para puxar de uma data específica
# d1 = datetime.datetime(2023, 12, 9).date()

pause_time = 10

datatxt, dataname = datef.dates(d1)

lista_clientes = ["ajobrand", "dadri", "infini", "mun", "othergirls", "una"]
# lista_clientes = ["othergirls"]

download_path = pathf.dl_folder()
final_path = rf"C:\Users\Mage Team\Documents\m&p\Relatorios Diarios\{dataname}"
if not os.path.exists(final_path):
    os.makedirs(final_path)
    print(f"The folder {final_path} was created.")


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


# %% CHROME

# Set up the Chrome WebDriver (make sure to provide the correct path to chromedriver.exe)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(
    "--start-maximized"
)  # Ensures that the window opens in full screen

#### Driver está na pasta C:\Program Files (x86)\Google\driver\
# Set up the Chrome WebDriver (make sure to provide the correct path to chromedriver.exe)
driver = webdriver.Chrome(options=chrome_options)

# Wait for some time to allow the page to load (you may need to adjust the sleep duration)
driver.implicitly_wait(5)
wait = WebDriverWait(driver, 10)  # 10 seconds timeout

for cliente in lista_clientes:
    df = pd.read_csv(
        rf"C:\Users\Mage Team\Documents\Txt\{cliente}_fb.txt", delimiter="\t"
    )
    url = df["info"][0]

    driver.get(url)

    filename_fb = f"{cliente}_Fb_{dataname}.csv"

    # %% EXPORTAR RELATÓRIO

    # Abrir menu
    time.sleep(pause_time)
    abrirmenu_xpath = "/html/body/app-bootstrap/ng2-bootstrap/lego-router-outlet/ng2-reporting-view/div/div[2]/div/ng2-reporting-plate/plate/div/div/div/div[1]/div/div/div/div/div/canvas-pancake-adapter/canvas-layout/div/div/div/div/div/ng2-report/ng2-canvas-container/div/div[1]/ng2-canvas-component/div/div/div/div/table-wrapper/div/ng2-table/div/div[3]/div[2]/div[2]/div[3]"
    element = driver.find_element(By.XPATH, abrirmenu_xpath)

    # Create an ActionChains object
    actions = ActionChains(driver)

    # Right-click on the element
    actions.context_click(element).perform()

    # Clicar em exportar
    time.sleep(pause_time - 8)
    exportar_xpath = "/html/body/div[3]/div[2]/div/div/div/span[3]/button"
    driver.find_element(By.XPATH, exportar_xpath).click()

    # Clicar no check box para manter a formatação de valor
    # Use the full XPath to locate the checkbox element
    time.sleep(pause_time - 8)
    checkbox_xpath = "/html/body/div[3]/div[2]/div/mat-dialog-container/div/div/data-export-dialog/div/mat-dialog-content/mat-checkbox/div/div/input"

    # Find the checkbox element using the XPath
    checkbox_element = driver.find_element(By.XPATH, checkbox_xpath)

    checkbox_element.click()

    # Clicar para download
    time.sleep(pause_time - 7)
    download1_xpath = "/html/body/div[3]/div[2]/div/mat-dialog-container/div/div/data-export-dialog/div/mat-dialog-actions/button[2]"
    driver.find_element(By.XPATH, download1_xpath).click()

    # Destino download e renomear

    time.sleep(pause_time - 5)
    rename_last_downloaded_file(download_path, final_path, filename_fb)

driver.quit()
