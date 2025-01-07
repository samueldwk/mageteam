# api_relger_eccossys_v1

import requests
import pandas as pd
import date_functions as datef
from datetime import timedelta, date
import datetime
import dotenv
import os

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime.datetime(2024, 1, 10).date()  # SELECT DATE

datatxt, dataname1, datasql, dataname2, dataname3 = datef.dates(d1)

# CLIENT LIST

c_list = [
    "ajobrand",
    "dadri",
    "french",
    "infini",
    "mun",
    "othergirls",
    "talgui",
    "una",
]

c_list = ["french"]

# API HEADER

payload = {}
files = {}


for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: GET LIST OF PRODUCTS (PAI)

    url_prod = "https://empresa.eccosys.com.br/api/produtos/produtosPai"
    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_prod = response_prod.json()
    df_ecco_prod = pd.DataFrame(dic_ecco_prod)

    # COLUMNS TO KEEP

    columns_to_keep = ["codigo", "precoCusto", "precoDe", "preco"]

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # In[2]: GET LIST OF ORDERS

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={dataname1}&$toDate={dataname1}&$offset=0&$count=50000&$dataConsiderada=data"

    response_ped = requests.request(
        "GET", url_ped, headers=headers, data=payload, files=files
    )

    dic_ecco_ped = response_ped.json()
    df_ecco_ped = pd.DataFrame(dic_ecco_ped)

    # CONVERT COLUMNS TYPE

    df_ecco_ped["desconto"] = df_ecco_ped["desconto"].str.replace(",", ".")

    columns_to_convert = ["totalProdutos", "desconto", "totalVenda", "frete"]
    df_ecco_ped[columns_to_convert] = df_ecco_ped[columns_to_convert].astype(
        float
    )

    # PRODUCTS SALES VALE WITHOUT DISCOUNT

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

    # COLUMNS TO KEEP

    columns_to_keep = [
        "id",
        "numeroPedido",
        "data",
        "desconto",
        "totalProdutos",
        "totalVenda",
        "frete",
        "primeiraCompra",
        "Status Name",
        "Vendas Produto Liquido",
    ]

    df_ecco_ped = df_ecco_ped[columns_to_keep]

    # KEEP ONLY THE ORDERS WITH STATUS NAME = Em aberto, Atendido e Pronto para picking

    status_name_keep = ["Em aberto", "Atendido", "Pronto para picking"]

    df_ecco_ped = df_ecco_ped[
        df_ecco_ped["Status Name"].isin(status_name_keep)
    ]

    # In[3]: CALL PRODUCTS FROM EACH ORDER AND MERGE IT WITH ORDERS DF AND CALCULATE SOME INDICATORS

    df_order_ids = df_ecco_ped["id"]

    df_ecco_ped_prod = pd.DataFrame()

    for order in df_order_ids:
        url_ped_prod = (
            f"https://empresa.eccosys.com.br/api/pedidos/{order}/items"
        )

        response_ped_prod = requests.request(
            "GET", url_ped_prod, headers=headers, data=payload, files=files
        )

        dic_ecco_ped_prod = response_ped_prod.json()
        df_ecco_ped_prod_un = pd.DataFrame(
            dic_ecco_ped_prod,
        )

        df_ecco_ped_prod = pd.concat(
            [df_ecco_ped_prod, df_ecco_ped_prod_un], ignore_index=True
        )

    # BRING PRODUCT COST

    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_prod, left_on="Cod. Modelo + Cor", right_on="codigo"
    )

    # CONVERT COLUMNS TYPE

    columns_to_convert = ["valor", "precoDe", "precoCusto", "quantidade"]
    df_ecco_ped_prod[columns_to_convert] = df_ecco_ped_prod[
        columns_to_convert
    ].astype(float)

    # CALCULATE DISCOUNT

    df_ecco_ped_prod["Desconto Produto"] = 1 - (
        df_ecco_ped_prod["valor"] / df_ecco_ped_prod["precoDe"]
    )

    # CALCULATE CMV FOR EACH PRODUCT SOLD PER ROW

    df_ecco_ped_prod["Custo Total Produto"] = (
        df_ecco_ped_prod["precoCusto"] * df_ecco_ped_prod["quantidade"]
    )

    # CALCULATE TOTAL SALES PER PRODUCT ROW

    df_ecco_ped_prod["Valor Total Venda Produto"] = (
        df_ecco_ped_prod["valor"] * df_ecco_ped_prod["quantidade"]
    )

    # INSERT DATE COLUMN

    df_ecco_ped_prod["Data"] = dataname1

    # In[4]: SALES PER DAY

    # CMV AND QUANTITY OF PRODUCTS SOLD

    grouped = df_ecco_ped_prod.groupby("Data")

    df_vendas_geral_1 = grouped.agg(
        {"Custo Total Produto": "sum", "quantidade": "sum"}
    )

    # SALES VALUE SUM

    grouped = df_ecco_ped.groupby("data")

    df_vendas_geral_2 = grouped.agg(
        {
            "totalVenda": "sum",
            "Vendas Produto Liquido": "sum",
            "desconto": "sum",
        }
    )

    # CONCAT BOTH DF VENDAS

    df_vendas_geral = pd.concat(
        [df_vendas_geral_1, df_vendas_geral_2], ignore_index=False, axis=1
    )

    # IMPUT MKP VENDAS COLUMN

    df_vendas_geral["MKP Vendas"] = (
        df_vendas_geral["Vendas Produto Liquido"]
        / df_vendas_geral["Custo Total Produto"]
    )

    # IMPUT ORDERS QTY

    df_vendas_geral["Quantidade de Pedidos"] = df_ecco_ped.shape[0]

    # IMPUT PRODUCTS PER ORDER AVERAGE

    df_vendas_geral["Peças por Atendimento"] = (
        df_vendas_geral["quantidade"]
        / df_vendas_geral["Quantidade de Pedidos"]
    )

    # AVERAGE TICKET

    df_vendas_geral["Ticket Médio"] = (
        df_vendas_geral["totalVenda"]
        / df_vendas_geral["Quantidade de Pedidos"]
    )

    # IMPUT DATANAME1 COLUMN

    df_vendas_geral["dataname1"] = dataname1

    # IMPUT YEAR AND MONTH COLUMNS

    df_vendas_geral["Mês"] = pd.to_datetime(
        df_vendas_geral["dataname1"]
    ).dt.month
    df_vendas_geral["Ano"] = pd.to_datetime(
        df_vendas_geral["dataname1"]
    ).dt.year

    # IMPUT DATATEXT1 COLUMN

    df_vendas_geral["datatxt"] = datatxt

    # ORDER COLUMNS

    df_vendas_geral = df_vendas_geral.loc[
        :,
        [
            "datatxt1",
            "Mês",
            "Ano",
            "totalVenda",
            "MKP Vendas",
            "Quantidade de Pedidos",
            "quantidade",
            "Peças por Atendimento",
            "Custo Total Produto",
            "Ticket Médio",
        ],
    ]

    # ORGANIZE DF TO CONCAT IT WITH THE OTHER DFS

    df_vendas_geral = df_vendas_geral.reset_index()
    df_vendas_geral = df_vendas_geral.drop("index", axis=1)

    # In[5]: SALES PER DISCOUNT

    # ROUND DESCONTO TO A REASONABLE VALUE

    precision = 5
    df_ecco_ped_prod.loc[:, "Desconto Produto"] = df_ecco_ped_prod[
        "Desconto Produto"
    ].round(precision)

    # CREATE BINS FOR EACH DISCOUNT RANGE

    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
    ]

    # ADD NEW COLUMN BASED ON DISCOUNT RANGE

    df_ecco_ped_prod.loc[:, "Faixa de Desconto"] = pd.cut(
        df_ecco_ped_prod["Desconto Produto"], bins=bins, labels=labels
    )

    # GROUPBY TO SUM SALES PER DISCOUNT RANGE

    result1 = (
        df_ecco_ped_prod.groupby("Faixa de Desconto", observed=False)[
            "Valor Total Venda Produto"
        ]
        .sum()
        .reset_index()
    )

    # CREATE DATA FRAME WITH RESULT FOR SALES PER DISCOUNT RANGE

    df_vendas_desconto = pd.DataFrame(result1)
    df_vendas_desconto = df_vendas_desconto.T

    # ORGANIZE DF TO CONCAT IT WITH THE OTHER DFS

    df_vendas_desconto.columns = df_vendas_desconto.iloc[0]
    df_vendas_desconto = df_vendas_desconto.drop("Faixa de Desconto")
    df_vendas_desconto = df_vendas_desconto.reset_index()
    df_vendas_desconto = df_vendas_desconto.drop("index", axis=1)

    # In[6]: GET STOCK PER PRODUCT

    # url_stock = f"https://empresa.eccosys.com.br/api/estoques?&$offset=0&$count=500000&$fromDate={dataname1}&$toDate={dataname1}"
    url_stock = (
        "https://empresa.eccosys.com.br/api/estoques&$toDate=2024-01-22"
    )
    response_stock = requests.request(
        "GET", url_stock, headers=headers, data=payload
    )

    dic_ecco_stock = response_stock.json()
    df_ecco_stock = pd.DataFrame(dic_ecco_stock)

    # BRING PRODUCT COST

    df_ecco_stock = df_ecco_stock.merge(
        df_ecco_prod, left_on="Cod. Modelo + Cor", right_on="codigo"
    )

    # CONVERT COLUMNS TYPE

    columns_to_convert = ["estoqueReal", "precoDe", "precoCusto", "preco"]
    df_ecco_stock[columns_to_convert] = df_ecco_stock[
        columns_to_convert
    ].astype(float)

    # CALCULATE DISCOUNT

    df_ecco_stock["Desconto Produto"] = 1 - (
        df_ecco_stock["preco"] / df_ecco_stock["precoDe"]
    )

    # CALCULATE STOCK COST PER PRODUCT

    df_ecco_stock["Custo Total Produto"] = (
        df_ecco_stock["precoCusto"] * df_ecco_stock["estoqueReal"]
    )

    # CALCULATE STOCK VALUE PER PRODUCT

    df_ecco_stock["Valor Estoque"] = (
        df_ecco_stock["preco"] * df_ecco_stock["estoqueReal"]
    )

    # GROUP BY COD. MODELO + COR (QTY, TOTAL COST AND STOCK VALUE)

    grouped = df_ecco_stock.groupby("Cod. Modelo + Cor")

    df_estoque_prod = grouped.agg(
        {
            "Custo Total Produto": "sum",
            "estoqueReal": "sum",
            "Valor Estoque": "sum",
            "estoqueDisponivel": "sum",
        }
    )

    # IMPUT DATE COLUMNS

    df_estoque_prod["dataname1"] = dataname1

    # In[7]: STOCK ON DAY (qty, total value and mkp)

    # GROUP BY dataname1 (QTY, TOTAL COST AND STOCK VALUE)

    grouped = df_estoque_prod.groupby("dataname1")

    df_estoque_geral = grouped.agg(
        {
            "Custo Total Produto": "sum",
            "estoqueReal": "sum",
            "Valor Estoque": "sum",
            "estoqueDisponivel": "sum",
        }
    )

    # MKP DO ESTOQUE

    df_estoque_geral["MKP Estoque"] = (
        df_estoque_geral["Valor Estoque"]
        / df_estoque_geral["Custo Total Produto"]
    )

    # ORDER COLUMNS

    df_estoque_geral = df_estoque_geral.loc[
        :, ["estoqueReal", "Valor Estoque", "MKP Estoque", "estoqueDisponivel"]
    ]

    # ORGANIZE DF TO CONCAT IT WITH THE OTHER DFS

    df_estoque_geral = df_estoque_geral.reset_index()
    df_estoque_geral = df_estoque_geral.drop("dataname1", axis=1)

    # In[7]: STOCK BY DISCOUNT

    # ROUND DESCONTO TO A REASONABLE VALUE

    precision = 5
    df_ecco_stock.loc[:, "Desconto Produto"] = df_ecco_stock[
        "Desconto Produto"
    ].round(precision)

    # CREATE BINS FOR EACH DISCOUNT RANGE

    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "E: <= 0%",
        "E: > 0% and <= 25%",
        "E: > 25% and <= 45%",
        "E: > 45% and <= 60%",
        "E: > 60%",
    ]

    # ADD NEW COLUMN BASED ON DISCOUNT RANGE

    df_ecco_stock.loc[:, "Faixa de Desconto"] = pd.cut(
        df_ecco_stock["Desconto Produto"], bins=bins, labels=labels
    )

    # GROUPBY TO SUM STOCK PER DISCOUNT RANGE

    result1 = (
        df_ecco_stock.groupby("Faixa de Desconto", observed=False)[
            "Valor Estoque"
        ]
        .sum()
        .reset_index()
    )

    # CREATE DATA FRAME WITH RESULT FOR STOCK PER DISCOUNT RANGE

    df_estoque_desconto = pd.DataFrame(result1)
    df_estoque_desconto = df_estoque_desconto.T

    # ORGANIZE DF TO CONCAT IT WITH THE OTHER DFS

    df_estoque_desconto.columns = df_estoque_desconto.iloc[0]
    df_estoque_desconto = df_estoque_desconto.drop("Faixa de Desconto")
    df_estoque_desconto = df_estoque_desconto.reset_index()
    df_estoque_desconto = df_estoque_desconto.drop("index", axis=1)

    # In[8]: CONCAT DFS (venda geral, venda desconto, estoque geral e estoque desconto)

    df_rel_ger = pd.concat(
        [
            df_vendas_geral,
            df_vendas_desconto,
            df_estoque_geral,
            df_estoque_desconto,
        ],
        axis=1,
    )
