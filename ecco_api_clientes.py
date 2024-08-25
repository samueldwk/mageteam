# rel_ger_ecco_api_v1.0
# Relatório de cliente

import requests
import pandas as pd
import datetime
import dotenv
import os

dotenv.load_dotenv()

datanamef = "2024-01-01"
datanamet = datetime.datetime.now()

# CLIENT LIST
c_list = ["una"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os pedidos

    url_client = "https://empresa.eccosys.com.br/api/clientes"

    response_client = requests.request(
        "GET", url_client, headers=headers, data=payload, files=files
    )

    dic_ecco_cliente = response_client.json()
    df_ecco_cliente = pd.DataFrame.from_dict(dic_ecco_cliente)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "cidade",
        "uf",
        "dataNascimento",
    ]

    df_ecco_cliente = df_ecco_cliente[columns_to_keep]

    # In[2]: Eccosys API: GET Listar todos os pedidos

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={datanamef}&$toDate={datanamet}&$offset=0&$count=50000&$dataConsiderada=data"

    response_ped = requests.request(
        "GET", url_ped, headers=headers, data=payload, files=files
    )

    dic_ecco_ped = response_ped.json()
    df_ecco_ped = pd.DataFrame.from_dict(dic_ecco_ped)

    # CONVERT COLUMNS TYPE
    df_ecco_ped["desconto"] = df_ecco_ped["desconto"].str.replace(",", ".")

    columns_to_convert = ["totalProdutos", "desconto", "totalVenda", "frete"]
    df_ecco_ped[columns_to_convert] = df_ecco_ped[columns_to_convert].astype(
        float
    )

    # PRODUCTS SALES VALUE WITHOUT DISCOUNT
    df_ecco_ped["Vendas Produto Liquido"] = (
        df_ecco_ped["totalProdutos"] - df_ecco_ped["desconto"]
    )

    # STATUS CODE X NAME
    dic_status_name = {
        "-1": "Aguardando pagamento",
        "0": "Em aberto",
        "1": "Atendido",
        "2": "Cancelado",
        "3": "Pronto para picking",
        "4": "Pagamento em análise",
    }

    df_status_name = pd.DataFrame(
        list(dic_status_name.items()), columns=["Status Code", "Status Name"]
    )

    # MERGE PEDIDOS WITH STATUS NAME ON SITUACAO
    df_ecco_ped = df_ecco_ped.merge(
        df_status_name, left_on="situacao", right_on="Status Code"
    )

    # KEEP ONLY THE ORDERS WITH STATUS NAME = Em aberto, Atendido e Pronto para picking
    status_name_keep = ["Em aberto", "Atendido", "Pronto para picking"]

    df_ecco_ped = df_ecco_ped[
        df_ecco_ped["Status Name"].isin(status_name_keep)
    ]

    # In[3]: Cálculos de cliente x pedidos

    # Agrupar por cliente e calcular a soma e a contagem das vendas
    df_ecco_ped_cliente = (
        df_ecco_ped.groupby("idContato")
        .agg(
            Total_Sales=("totalVenda", "sum"),
            Sales_Count=("totalVenda", "size"),
        )
        .reset_index()
    )

    # Trazer inf. do cliente para tabela de venda por cliente

    df_ecco_ped_cliente_completo = pd.merge(
        df_ecco_ped_cliente,
        df_ecco_cliente,
        left_on="idContato",
        right_on="id",
        how="left",
    )

    # Substituir strings 'None' por valores NaN para facilitar a conversão
    df_ecco_ped_cliente_completo[
        "dataNascimento"
    ] = df_ecco_ped_cliente_completo["dataNascimento"].replace("None", pd.NA)

    # Converter a coluna aniversário para o formato de data
    df_ecco_ped_cliente_completo["dataNascimento"] = pd.to_datetime(
        df_ecco_ped_cliente_completo["dataNascimento"], errors="coerce"
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
    df_ecco_ped_cliente_completo["Idade"] = df_ecco_ped_cliente_completo[
        "dataNascimento"
    ].apply(calculate_age)

    # Agrupar venda por idade
    df_ecco_ped_idade = (
        df_ecco_ped_cliente_completo.groupby("Idade")
        .agg(
            Total_Sales=("Total_Sales", "sum"),
            Sales_Count=("Sales_Count", "sum"),
        )
        .reset_index()
    )

    # Cálcular Ticket Médio por Idade

    df_ecco_ped_idade["Ticket Médio"] = (
        df_ecco_ped_idade["Total_Sales"] / df_ecco_ped_idade["Sales_Count"]
    )

    # Agrupar venda por uf
    df_ecco_ped_uf = (
        df_ecco_ped_cliente_completo.groupby("uf")
        .agg(
            Total_Sales=("Total_Sales", "sum"),
            Sales_Count=("Sales_Count", "sum"),
        )
        .reset_index()
    )

    # Cálcular Ticket Médio por uf

    df_ecco_ped_uf["Ticket Médio"] = (
        df_ecco_ped_uf["Total_Sales"] / df_ecco_ped_uf["Sales_Count"]
    )
