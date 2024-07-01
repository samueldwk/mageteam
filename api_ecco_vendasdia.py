# VENDAS POR PRODUTO DIÁRIO

# TESTE API CLIENTE MUN

import requests
import pandas as pd
import date_functions as datef
from datetime import timedelta, date
import datetime
import dotenv
import os
import gspread


dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime.datetime(2024, 5, 1).date()  # SELECT DATE

datatxt1, dataname1, datasql = datef.dates(d1)

# CLIENT LIST

c_list = [
    # "ajobrand",
    "alanis",
    "dadri",
    "french",
    "haverroth",
    "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "talgui",
    "paconcept",
    "una",
]

c_list = ["mun"]

# NAME DICTIONARY

dic_nomes = {
    "ajobrand": "aJo Brand",
    "alanis": "Alanis",
    "dadri": "Dadri",
    "french": "French",
    "haverroth": "Haverroth",
    "infini": "Infini",
    "kle": "Kle",
    "luvic": "Luvic",
    "mun": "Mun",
    "nobu": "Nobu",
    "othergirls": "Other Girls",
    "paconcept": "P.A Concept",
    "talgui": "Talgui",
    "una": "Una",
}


# DICTS TO SLICE CODIGO SKU

c_list_sku_0 = {
    "Cliente": ["mun", "infini", "luvic", "haverroth"],
    "Index": 0,
}
c_list_sku_7 = {
    "Cliente": ["nobu"],
    "Index": 7,
}
c_list_sku_8 = {
    "Cliente": ["alanis", "dadri", "othergirls", "talgui"],
    "Index": 8,
}
c_list_sku_9 = {
    "Cliente": ["ajobrand", "french", "una"],
    "Index": 9,
}
c_list_sku_15 = {
    "Cliente": ["kle", "paconcept"],
    "Index": 15,
}  ###get all sku, doesnt need to slice

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

    # TRANSFORM SKU TO COD. MODELO + COR

    c_list_sku = [
        c_list_sku_0,
        c_list_sku_7,
        c_list_sku_8,
        c_list_sku_9,
        c_list_sku_15,
    ]

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

    # INSERT DATE COLUMN

    df_ecco_ped_prod["Data"] = dataname1

    # In[4]: PRODUCT SALES PER DAY

    # COLUMNS TO KEEP

    columns_to_keep = ["Data", "Cod. Modelo + Cor", "quantidade"]

    df_ecco_venda_produto = df_ecco_ped_prod[columns_to_keep]

    # In[5]: UPDATE GOOGLE SHEETS

    gc = gspread.oauth()

    sh = gc.open(f"{dic_nomes[cliente]} - Venda de Produto Diário").worksheet(
        "DB - Vendas"
    )

    df_ecco_venda_produto = df_ecco_venda_produto.values.tolist()

    print(datatxt1)

    sh.append_rows(df_ecco_venda_produto, table_range="A1")

    # # In[6]: GET STOCK PER PRODUCT

    # # url_stock = f"https://empresa.eccosys.com.br/api/estoques?&$offset=0&$count=500000&$fromDate={dataname1}&$toDate={dataname1}"
    # url_stock = (
    #     f"https://empresa.eccosys.com.br/api/estoques&$toDate={dataname1}"
    # )
    # response_stock = requests.request(
    #     "GET", url_stock, headers=headers, data=payload
    # )

    # dic_ecco_stock = response_stock.json()
    # df_ecco_stock = pd.DataFrame(dic_ecco_stock)

    # # TRANSFORM SKU TO COD. MODELO + COR

    # c_list_sku = [c_list_sku_0, c_list_sku_8, c_list_sku_9]

    # for cliente in c_list:
    #     for c_sku in c_list_sku:
    #         if cliente in c_sku["Cliente"]:
    #             index = c_sku["Index"]
    #             if index == 0:
    #                 df_ecco_stock["Cod. Modelo + Cor"] = df_ecco_stock[
    #                     "codigo"
    #                 ].apply(lambda x: x.split("-")[0])
    #             else:
    #                 df_ecco_stock["Cod. Modelo + Cor"] = df_ecco_stock[
    #                     "codigo"
    #                 ].str.slice(0, index)
    #             break
    #     else:
    #         print(f"The value '{cliente}' is not present in any c_list_sku.")

    # # BRING PRODUCT COST

    # df_ecco_stock = df_ecco_stock.merge(
    #     df_ecco_prod, left_on="Cod. Modelo + Cor", right_on="codigo"
    # )

    # # CONVERT COLUMNS TYPE

    # columns_to_convert = ["estoqueReal", "precoDe", "precoCusto", "preco"]
    # df_ecco_stock[columns_to_convert] = df_ecco_stock[
    #     columns_to_convert
    # ].astype(float)

    # # CALCULATE DISCOUNT

    # df_ecco_stock["Desconto Produto"] = 1 - (
    #     df_ecco_stock["preco"] / df_ecco_stock["precoDe"]
    # )

    # # CALCULATE STOCK COST PER PRODUCT

    # df_ecco_stock["Custo Total Produto"] = (
    #     df_ecco_stock["precoCusto"] * df_ecco_stock["estoqueReal"]
    # )

    # # CALCULATE STOCK VALUE PER PRODUCT

    # df_ecco_stock["Valor Estoque"] = (
    #     df_ecco_stock["preco"] * df_ecco_stock["estoqueReal"]
    # )

    # # GROUP BY COD. MODELO + COR (QTY, TOTAL COST AND STOCK VALUE)

    # grouped = df_ecco_stock.groupby("Cod. Modelo + Cor")

    # df_estoque_prod = grouped.agg(
    #     {
    #         "Custo Total Produto": "sum",
    #         "estoqueReal": "sum",
    #         "Valor Estoque": "sum",
    #         "estoqueDisponivel": "sum",
    #     }
    # )

    # # IMPUT DATE COLUMNS

    # df_estoque_prod["dataname1"] = dataname1

    # # In[7]: STOCK (qty, total value and mkp)

    # # GROUP BY dataname1 (QTY, TOTAL COST AND STOCK VALUE)

    # grouped = df_estoque_prod.groupby("dataname1")

    # df_estoque_geral = grouped.agg(
    #     {
    #         "Custo Total Produto": "sum",
    #         "estoqueReal": "sum",
    #         "Valor Estoque": "sum",
    #         "estoqueDisponivel": "sum",
    #     }
    # )

    # # MKP DO ESTOQUE

    # df_estoque_geral["MKP Estoque"] = (
    #     df_estoque_geral["Valor Estoque"]
    #     / df_estoque_geral["Custo Total Produto"]
    # )

    # # ORDER COLUMNS

    # df_estoque_geral = df_estoque_geral.loc[
    #     :, ["estoqueReal", "Valor Estoque", "MKP Estoque", "estoqueDisponivel"]
    # ]

    # # ORGANIZE DF TO CONCAT IT WITH THE OTHER DFS

    # df_estoque_geral = df_estoque_geral.reset_index()
    # df_estoque_geral = df_estoque_geral.drop("dataname1", axis=1)

    # # In[8]: CONCAT DFS (venda geral, venda desconto, estoque geral e estoque desconto)
