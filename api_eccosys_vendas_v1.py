# api_eccosys_vendas_v1 (3 dias de venda anterior) por pedido e por produto

import requests
import pandas as pd
import date_functions as datef
from datetime import timedelta, date
import dotenv
import os
import time


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

    # In[1]: GET LIST OF ORDERS

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={dataname3}&$toDate={dataname1}&$offset=0&$count=50000&$dataConsiderada=data"

    response_ped = requests.request(
        "GET", url_ped, headers=headers, data=payload, files=files
    )

    dic_ecco_ped = response_ped.json()
    df_ecco_ped = pd.DataFrame(dic_ecco_ped)

    # PRIMEIRA COMPRA 1 = SIM , VAZIO = NAO

    df_ecco_ped["primeiraCompra"] = df_ecco_ped["primeiraCompra"].replace(
        {"1": "sim", "": "nao"}
    )

    # CONVERT COLUMNS TYPE

    df_ecco_ped["desconto"] = df_ecco_ped["desconto"].str.replace(",", ".")

    columns_to_convert = ["totalProdutos", "desconto", "totalVenda", "frete"]
    df_ecco_ped[columns_to_convert] = df_ecco_ped[columns_to_convert].astype(
        float
    )

    # columns_to_convert = ["id", "idContato", "numeroPedido", "primeiraCompra"]
    # df_ecco_ped[columns_to_convert] = df_ecco_ped[columns_to_convert].astype(
    #     int
    # )

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
        "idContato",
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

    # RENAME COLUMNS NAME

    df_ecco_ped = df_ecco_ped.rename(
        columns={
            "id": "idVenda",
            "idContato": "idCliente",
            "numeroPedido": "NumeroPedido",
            "data": "DataVendaPedido",
            "desconto": "DescontoPedido",
            "totalProdutos": "ValorVendaProdutoBruto",
            "totalVenda": "ValorTotalVenda",
            "frete": "ValorFrete",
            "primeiraCompra": "PrimeiraCompra",
            "Status Name": "StatusPedido",
            "Vendas Produto Liquido": "ValorVendaProdutoLiquido",
        }
    )

    df_ecco_ped.dtypes

    # In[2]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_ped = df_ecco_ped.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"mage_eccosys_pedidos_{client}_v1")
            .upsert(dic_ecco_ped)
            .execute()
        )

    except Exception as exception:
        print(exception)

    # In[3]: CALL PRODUCTS FROM EACH ORDER

    df_order_ids = df_ecco_ped["idVenda"]

    df_ecco_ped_prod = pd.DataFrame()

    for order in df_order_ids:
        url_ped_prod = (
            f"https://empresa.eccosys.com.br/api/pedidos/{order}/items"
        )

        while True:
            response_ped_prod = requests.get(
                url_ped_prod, headers=headers, data=payload, files=files
            )

            if response_ped_prod.status_code == 429:
                print(
                    f"Received 429 status for order {order}. Retrying after 10 seconds..."
                )
                time.sleep(10)  # Wait for 10 seconds before retrying
            else:
                # If the status code is not 429, break out of the loop
                break

        # Proceed if the response is successful
        if response_ped_prod.status_code == 200:
            dic_ecco_ped_prod = response_ped_prod.json()
            df_ecco_ped_prod_un = pd.DataFrame(dic_ecco_ped_prod)

            df_ecco_ped_prod = pd.concat(
                [df_ecco_ped_prod, df_ecco_ped_prod_un], ignore_index=True
            )
        else:
            print(
                f"Failed to retrieve data for order {order}. Status code: {response_ped_prod.status_code}"
            )

    # CONVERT COLUMNS TYPE

    columns_to_convert = ["valor", "valorDesconto", "quantidade"]

    df_ecco_ped_prod[columns_to_convert] = df_ecco_ped_prod[
        columns_to_convert
    ].astype(float)

    columns_to_convert = ["quantidade"]

    df_ecco_ped_prod[columns_to_convert] = df_ecco_ped_prod[
        columns_to_convert
    ].astype(int)

    # COLOCAR DATA DO PEDIDO PARA CADA PRODUTO VENDIDO

    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_ped, how="left", left_on="idVenda", right_on="idVenda"
    )

    # COLUMNS TO KEEP

    columns_to_keep = [
        "idVenda",
        "idProduto",
        "quantidade",
        "valor",
        "descricao",
        "valorDesconto",
        "codigo",
        "DataVendaPedido",
    ]

    df_ecco_ped_prod = df_ecco_ped_prod[columns_to_keep]

    # RENAME COLUMNS NAME

    df_ecco_ped_prod = df_ecco_ped_prod.rename(
        columns={
            "idVenda": "idPedido",
            "quantidade": "QuantidadeVenda",
            "valor": "ValorVendaUnit",
            "valorDesconto": "ValorDescontoPedido",
            "descricao": "NomeProduto",
            "codigo": "CodigoProduto",
        }
    )

    # In[4]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_ped_prod = df_ecco_ped_prod.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"mage_eccosys_vendas_produto_{client}_v1")
            .upsert(dic_ecco_ped_prod)
            .execute()
        )

    except Exception as exception:
        print(exception)
