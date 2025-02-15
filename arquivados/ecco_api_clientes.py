# rel_ger_ecco_api_v1.0
# Relatório de cliente

import requests
import pandas as pd
import datetime
import dotenv
import os
import time


dotenv.load_dotenv()

datanamef = "2021-01-01"
datanamet = datetime.datetime.now().date()

# CLIENT LIST
c_list = ["mun"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os clientes

    url_client = "https://empresa.eccosys.com.br/api/clientes"

    response_client = requests.request(
        "GET", url_client, headers=headers, data=payload, files=files
    )

    dic_ecco_cliente = response_client.json()
    df_ecco_cliente = pd.DataFrame.from_dict(dic_ecco_cliente)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "nome",
        "cep",
        "cidade",
        "uf",
        "dataNascimento",
        "celular",
        "email",
    ]

    df_ecco_cliente = df_ecco_cliente[columns_to_keep]

    # In[2]: Tratar dados

    # Substituir strings 'None' por valores NaN para facilitar a conversão
    df_ecco_cliente["dataNascimento"] = df_ecco_cliente[
        "dataNascimento"
    ].replace("None", pd.NA)

    # Converter a coluna aniversário para o formato de data
    df_ecco_cliente["dataNascimento"] = pd.to_datetime(
        df_ecco_cliente["dataNascimento"], errors="coerce"
    )

    def calculate_age(birth_date):
        if pd.isna(birth_date):
            return None
        age = datanamet.year - birth_date.year
        # Ajustar se a data de aniversário ainda não ocorreu este ano
        if (datanamet.month, datanamet.day) < (
            birth_date.month,
            birth_date.day,
        ):
            age -= 1
        return age

    # Aplicar a função para calcular a idade
    df_ecco_cliente["Idade"] = df_ecco_cliente["dataNascimento"].apply(
        calculate_age
    )

    # In[6]: Exportar planilhas para Excel

    # Set the directory where you want to save the file
    directory = r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"

    # Construct the full file path
    file_path = os.path.join(
        directory, f"cliente_{client}_({datanamef})-({datanamet}).xlsx"
    )

    # Export df ped_cliente_completo
    df_ecco_cliente.to_excel(file_path, index=False)

    print(
        f"Export Complete: cliente_{client}_({datanamef})-({datanamet}).xlsx"
    )
