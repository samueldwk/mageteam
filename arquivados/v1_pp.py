# PERFORMANCE DE PRODUTO

import requests
import pandas as pd
import date_functions as datef
from datetime import timedelta, date
import datetime
import time
import os
import dotenv

dotenv.load_dotenv()


# DATE FUCTIONS

d2 = datetime.datetime(2024, 1, 8).date()  # SELECT FROM DATE
d1 = datetime.datetime(2024, 1, 14).date()  # SELECT TO DATE

datatxt2, dataname2, datasql2 = datef.dates(d2)
datatxt1, dataname1, datasql1 = datef.dates(d1)


# CLIENT LIST

c_list = ["french"]

# DICTS PARA SLICE DO CODIGO SKU

c_list_sku_0 = {"Cliente": ["mun", "infini"], "Index": 0}
c_list_sku_8 = {"Cliente": ["talgui", "dadri", "othergirls"], "Index": 8}
c_list_sku_9 = {"Cliente": ["ajobrand", "french", "una"], "Index": 9}

for client in c_list:
    # API HEADER

    payload = {}
    files = {}

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

    # CONVERT COLUMNS TYPE

    columns_to_convert = ["precoCusto", "precoDe", "preco"]
    df_ecco_prod[columns_to_convert] = df_ecco_prod[columns_to_convert].astype(
        float
    )

    # ADD DISCOUNT COLUMNS

    df_ecco_prod["Desconto"] = 1 - (
        df_ecco_prod["preco"] / df_ecco_prod["precoDe"]
    )

    # COLUMNS TO KEEP

    columns_to_keep = [
        "codigo",
        "precoCusto",
        "precoDe",
        "preco",
        "nome",
        "Desconto",
    ]

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # In[2]: GET LIST OF ORDERS

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={dataname2}&$toDate={dataname1}&$offset=0&$count=50000&$dataConsiderada=data"

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
        time.sleep(0.5)

    # TRANSFORM SKU TO COD. MODELO + COR

    c_list_sku = [c_list_sku_0, c_list_sku_8, c_list_sku_9]

    for cliente in c_list:
        for c_sku in c_list_sku:
            if cliente in c_sku["Cliente"]:
                index = c_sku["Index"]
                if index == 0:
                    df_ecco_ped_prod["Cod. Modelo + Cor"] = df_ecco_ped_prod[
                        "codigo"
                    ].apply(lambda x: x.split("-")[0])
                else:
                    df_ecco_ped_prod["Cod. Modelo + Cor"] = df_ecco_ped_prod[
                        "codigo"
                    ].str.slice(0, index)
                break
        else:
            print(f"The value '{cliente}' is not present in any c_list_sku.")

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

    # IMPUT IN 'df_ecco_ped_prod' sales date

    df_ecco_ped_prod = pd.merge(
        df_ecco_ped_prod,
        df_ecco_ped[["id", "data"]],
        left_on="idVenda",
        right_on="id",
        how="left",
    )

    # TRANSFORM 'data' to datetime format

    df_ecco_ped_prod["data"] = pd.to_datetime(df_ecco_ped_prod["data"])

    # In[4]: SALES BY PRODUCT DF

    # GROUP BY 'COD. MODELO + COR' and 'data'

    grouped = df_ecco_ped_prod.groupby(["Cod. Modelo + Cor", "data"])
    df_vendas_produto = grouped.agg({"quantidade": "sum"}).reset_index()

    # ADD COLUMN WEEK NUMBER

    df_vendas_produto["Semana"] = (
        df_vendas_produto["data"].dt.strftime("%U").astype(int) + 1
    )

    # In[5]: STOCK

    # In[6]: CREATE FINAL DF 'df_performance_produto'

    # REORGANIZE VIEW

    df_performance_produto = pd.pivot_table(
        df_vendas_produto,
        values="quantidade",
        index="Cod. Modelo + Cor",
        columns="Semana",
        aggfunc="sum",
        fill_value=0,
    )

    # FIRST AND LAST DAY OF THE WEEK TO RENAME COLUMNS 'Semana'

    week_first_last = df_vendas_produto.groupby("Semana")["data"].agg(
        ["min", "max"]
    )

    # RENAME COLUMNS IN 'df_performance_produto

    df_performance_produto.columns = [
        f'{week_first_last.at[col, "min"].strftime("%Y-%m-%d")} - {week_first_last.at[col, "max"].strftime("%Y-%m-%d")}'
        for col in df_performance_produto.columns
    ]

    # RESET INDEX

    df_performance_produto.reset_index(inplace=True)

    # BRING PRODUCT INFORMATION TO 'df_vendas_produto_view'

    df_performance_produto = pd.merge(
        df_performance_produto,
        df_ecco_prod,
        left_on="Cod. Modelo + Cor",
        right_on="codigo",
    )

    # ORGANIZE COLUMNS

    # columns_order = [0, 8, 5, 6, 7, 9, 1, 2, 3]
    # df_performance_produto_final = df_performance_produto.iloc[:, columns_order]

    # In[7]: EXPORT 'df_performance_produto_final'
