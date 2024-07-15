# rel_ger_ecco_api_v1.0
# REL GER ECCOSYS BY API

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time

dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt1, dataname1, datasql, dataname2 = datef.dates(d1)

# CLIENT LIST
c_list = [
    # "ajobrand",
    "alanis",
    # "dadri",
    "french",
    # "haverroth",
    "haut",
    # "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    # "talgui",
    "paconcept",
    "una",
]

# c_list = ["kle"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$opcEcommerce=S"
    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_prod = response_prod.json()
    df_ecco_prod = pd.DataFrame.from_dict(dic_ecco_prod)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "codigo",
        "precoCusto",
        "precoDe",
        "preco",
        "situacao",
    ]

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # In[2]: Eccosys API: GET Listar todos os pedidos

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={dataname1}&$toDate={dataname1}&$offset=0&$count=50000&$dataConsiderada=data"

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

    # In[3]: Eccosys API: GET listar todos os itens de um pedido
    # CALL PRODUCTS FROM EACH ORDER AND MERGE IT WITH ORDERS DF AND CALCULATE SOME INDICATORS

    df_order_ids = df_ecco_ped["id"]
    df_ecco_ped_prod = pd.DataFrame()

    max_requests_per_minute = 60
    sleep_time = (
        60 / max_requests_per_minute
    )  # Time to wait between requests in seconds

    def make_request_with_retries(url, headers, data, files, max_retries=10):
        retries = 0
        backoff_time = 1  # Initial backoff time in seconds
        while retries < max_retries:
            response = requests.get(
                url, headers=headers, data=data, files=files
            )
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Too many requests
                retries += 1
                time.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
            else:
                response.raise_for_status()
        raise Exception(
            f"Failed to fetch data from {url} after {max_retries} retries"
        )

    for order in df_order_ids:
        url_ped_prod = (
            f"https://empresa.eccosys.com.br/api/pedidos/{order}/items"
        )

        response_ped_prod = make_request_with_retries(
            url_ped_prod, headers=headers, data=payload, files=files
        )

        dic_ecco_ped_prod = response_ped_prod.json()
        df_ecco_ped_prod_un = pd.DataFrame.from_dict(
            dic_ecco_ped_prod,
        )

        df_ecco_ped_prod = pd.concat(
            [df_ecco_ped_prod, df_ecco_ped_prod_un], ignore_index=True
        )

    # BRING PRODUCT COST
    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_prod, left_on="idProduto", right_on="id"
    )

    # CONVERT COLUMNS TYPE
    columns_to_convert = [
        "valor",
        "precoDe",
        "precoCusto",
        "quantidade",
        "valorDesconto",
        "precoLista",
    ]
    df_ecco_ped_prod[columns_to_convert] = df_ecco_ped_prod[
        columns_to_convert
    ].astype(float)

    # CALCULATE DISCOUNT
    df_ecco_ped_prod["Desconto Produto"] = 1 - (
        df_ecco_ped_prod["precoLista"] / df_ecco_ped_prod["precoDe"]
    )

    # CALCULATE CMV FOR EACH PRODUCT SOLD PER ROW
    df_ecco_ped_prod["Custo Total Produto"] = (
        df_ecco_ped_prod["precoCusto"] * df_ecco_ped_prod["quantidade"]
    )

    # CALCULATE TOTAL SALES PER PRODUCT ROW
    df_ecco_ped_prod["Valor Total Venda Produto"] = (
        df_ecco_ped_prod["precoLista"] * df_ecco_ped_prod["quantidade"]
    )

    # INSERT DATE COLUMN
    df_ecco_ped_prod["Data"] = dataname1

    # CALCULATE "Vendas Produto Liquido"(Valor total venda produto - valor total desconto de pedidos pagos)
    df_ecco_ped_prod["Valor Total Venda Produto Liquido"] = (
        df_ecco_ped_prod["Valor Total Venda Produto"]
        - df_ecco_ped_prod["valorDesconto"]
    )

    # In[4]: REL GER: CONSTRUCT TOTAL SALES PART

    # SUM "totalVenda"(Valor de venda total de pedidos pagos)
    resultado_valor_vendas_total = df_ecco_ped["totalVenda"].sum()

    # SUM "Custo Total Produto"(Valor de custo total dos produtos vendidos pagos)
    resultado_valor_cmv_total = df_ecco_ped_prod["Custo Total Produto"].sum()

    # SUM "Vendas Produto Liquido"(Valor total venda produto - valor total desconto de pedidos pagos)
    resultado_valor_vendas_prod_liq_total_02 = df_ecco_ped[
        "Vendas Produto Liquido"
    ].sum()

    # CALCULATE "MKP de Vendas" ((Valor vendas produto - Desconto Pedido) / CMV)
    resultado_mkp_vendas_total = (
        resultado_valor_vendas_prod_liq_total_02 / resultado_valor_cmv_total
    )
    resultado_mkp_vendas_total = round(resultado_mkp_vendas_total, 2)

    # CALCULATE "Quantidade Peças Vendidas Total"
    resultado_quantidade_peças_vendidas_total = df_ecco_ped_prod[
        "quantidade"
    ].sum()

    # CALCULATE "Preço Médio Vendido Total" ("Vendas Produto Liquido" / "Quantidade Peças Vendidas Total")
    resultado_preco_medio_vendido_total = (
        resultado_valor_vendas_prod_liq_total_02
        / resultado_quantidade_peças_vendidas_total
    )
    resultado_preco_medio_vendido_total = round(
        resultado_preco_medio_vendido_total, 2
    )

    # CALCULATE "Quantidade de Pedidos Vendidos Total"
    resultado_quantidade_pedidos_total = df_ecco_ped["id"].count()

    # CALCULATE "Ticket Médio"
    resultado_ticketmedio = (
        resultado_valor_vendas_total / resultado_quantidade_pedidos_total
    )
    resultado_ticketmedio = round(resultado_ticketmedio, 2)

    # CREATE df_relger_vendas_total
    df_relger_vendas_total = pd.DataFrame.from_dict(
        {
            "DATA": [dataname1],
            "VALOR PEDIDOS APROVADOS": [resultado_valor_vendas_total],
            "CMV": [resultado_valor_cmv_total],
            "MKP VENDAS": [resultado_mkp_vendas_total],
            "QUANTIDADE PEDIDOS APROVADOS": [
                resultado_quantidade_pedidos_total
            ],
            "QUANTIDADE DE PRODUTOS APROVADOS": [
                resultado_quantidade_peças_vendidas_total
            ],
            "PREÇO MÉDIO VENDAS": [resultado_preco_medio_vendido_total],
            "TICKET MÉDIO": [resultado_ticketmedio],
        }
    )

    # In[5]: REL GER: Calculate sales per product discount range

    # Round the 'Desconto' values to a reasonable precision
    precision = 5  # Adjust the precision as needed
    df_ecco_ped_prod.loc[:, "Desconto Produto"] = df_ecco_ped_prod[
        "Desconto Produto"
    ].round(precision)

    # Create bins for the 'Desconto Produto' column
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
    ]

    # Add a new column 'Faixa de Desconto' based on the bins
    df_ecco_ped_prod.loc[:, "Faixa de Desconto"] = pd.cut(
        df_ecco_ped_prod["Desconto Produto"], bins=bins, labels=labels
    )

    # SUBSTITUTE ANY NAN BY 0 IN COLUMN "Desconto Produto"
    df_ecco_ped_prod["Faixa de Desconto"] = df_ecco_ped_prod[
        "Faixa de Desconto"
    ].fillna("V: <= 0%")

    # Use groupby to sum 'Valor de Venda' for each 'Faixa de Desconto'
    resultado_valor_vendas_faixa_desconto = (
        df_ecco_ped_prod.groupby("Faixa de Desconto", observed=False)[
            "Valor Total Venda Produto Liquido"
        ]
        .sum()
        .reset_index()
    )

    # Create a new DataFrame 'df_relger_vendas_pordesconto_final' with the results
    df_vendas_pordesconto_bruto = pd.DataFrame.from_dict(
        resultado_valor_vendas_faixa_desconto
    )
    df_relger_vendas_pordesconto_final = df_vendas_pordesconto_bruto.T
    df_relger_vendas_pordesconto_final.columns = (
        df_relger_vendas_pordesconto_final.iloc[0]
    )
    df_relger_vendas_pordesconto_final = (
        df_relger_vendas_pordesconto_final.iloc[1:].copy()
    )
    df_relger_vendas_pordesconto_final = (
        df_relger_vendas_pordesconto_final.reset_index(drop=True)
    )

    # In[6]: Eccosys API: GET Listar quantidades em estoque

    url_estoque = "https://empresa.eccosys.com.br/api/estoques?&$offset=0&$count=10000000"

    response_estoque = requests.request(
        "GET", url_estoque, headers=headers, data=payload, files=files
    )

    contador = 0

    while response_estoque.status_code != 200:
        print(
            f"{client}: Status_code response = {response_estoque.status_code}"
        )

        time.sleep(300)

        contador = contador + 1

        response_estoque = requests.request(
            "GET", url_estoque, headers=headers, data=payload, files=files
        )

        if contador == 60:
            print(f"{client}: request url_estoque reached try limit")

            break

    dic_ecco_estoque = response_estoque.json()
    df_ecco_estoque_cru = pd.DataFrame.from_dict(dic_ecco_estoque)

    # SUBSTITUTE ALL NEGATIVE VALUES BY 0
    df_ecco_estoque_limpo = df_ecco_estoque_cru.copy()

    df_ecco_estoque_limpo["estoqueDisponivel"] = df_ecco_estoque_limpo[
        "estoqueDisponivel"
    ].apply(lambda x: x if x > 0 else 0)

    df_ecco_estoque_prod = df_ecco_estoque_limpo.merge(
        df_ecco_prod, on="codigo", how="left"
    )

    # In[7]: REL GER: CONSTRUCT TOTAL STOCK PART (ESTOQUE DISPONIVEL)

    # CONVERT COLUMNS TYPE
    columns_to_convert = [
        "precoDe",
        "precoCusto",
        "preco",
    ]
    df_ecco_estoque_prod[columns_to_convert] = df_ecco_estoque_prod[
        columns_to_convert
    ].astype(float)

    # REMOVER ESTOQUE DE EMBALAGENS / VALE PRESENTES (TUDO QUE NÃO FOR PRODUTO ACABADO PRINCIPAL)

    if client == "mun":
        df_ecco_estoque_prod = df_ecco_estoque_prod[
            ~df_ecco_estoque_prod.apply(
                lambda row: row.astype(str).str.contains("VP").any(), axis=1
            )
        ]

    # SUM TOTAL QTY OF STOCK
    resultado_quantidade_estoque_total = df_ecco_estoque_prod[
        "estoqueDisponivel"
    ].sum()

    # CALCULATE TOTAL STOCK VALUE
    resultado_valor_estoque_total = (
        df_ecco_estoque_prod["estoqueDisponivel"]
        * df_ecco_estoque_prod["preco"]
    ).sum()

    # CALCULATE "Preço médio do produto do estoque"
    resultado_preco_medio_estoque = (
        resultado_valor_estoque_total / resultado_quantidade_estoque_total
    )

    # CALCULATE "Custo total do estoque"
    resultado_custo_estoque_total = (
        df_ecco_estoque_prod["estoqueDisponivel"]
        * df_ecco_estoque_prod["precoCusto"]
    ).sum()

    # CALCULATE "MKP do estoque"
    resultado_mkp_estoque_total = (
        resultado_valor_estoque_total / resultado_custo_estoque_total
    )

    # CREATE df_relger_estoque_total
    df_relger_estoque_total = pd.DataFrame.from_dict(
        {
            "QUANTIDADE ESTOQUE TOTAL": [resultado_quantidade_estoque_total],
            "VALOR ESTOQUE TOTAL": [resultado_valor_estoque_total],
            "PREÇO MÉDIO ESTOQUE TOTAL": [resultado_preco_medio_estoque],
            "MKP ESTOQUE TOTAL": [resultado_mkp_estoque_total],
        }
    )

    # In[8]: REL GER: Calculate stock per product discount range

    # CALCULATE DISCOUNT
    df_ecco_estoque_prod["Desconto Produto"] = 1 - (
        df_ecco_estoque_prod["preco"] / df_ecco_estoque_prod["precoDe"]
    )

    # Round the 'Desconto' values to a reasonable precision
    precision = 5  # Adjust the precision as needed
    df_ecco_estoque_prod.loc[:, "Desconto Produto"] = df_ecco_estoque_prod[
        "Desconto Produto"
    ].round(precision)

    # Create bins for the 'Desconto Produto' column
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "E: <= 0%",
        "E: > 0% and <= 25%",
        "E: > 25% and <= 45%",
        "E: > 45% and <= 60%",
        "E: > 60%",
    ]

    # Add a new column 'Faixa de Desconto' based on the bins
    df_ecco_estoque_prod.loc[:, "Faixa de Desconto"] = pd.cut(
        df_ecco_estoque_prod["Desconto Produto"], bins=bins, labels=labels
    )

    # SUBSTITUTE ANY NAN BY 0 IN COLUMN "Desconto Produto"
    df_ecco_estoque_prod["Faixa de Desconto"] = df_ecco_estoque_prod[
        "Faixa de Desconto"
    ].fillna("E: <= 0%")

    # CALCULATE STOCK VALUE PER PRODUCT
    df_ecco_estoque_prod["Valor Estoque por Produto"] = (
        df_ecco_estoque_prod["estoqueDisponivel"]
        * df_ecco_estoque_prod["preco"]
    )

    # Use groupby to sum 'Valor de Estoque' for each 'Faixa de Desconto'
    resultado_valor_estoque_faixa_desconto = (
        df_ecco_estoque_prod.groupby("Faixa de Desconto", observed=False)[
            "Valor Estoque por Produto"
        ]
        .sum()
        .reset_index()
    )

    # Create a new DataFrame 'df_relger_estoque_pordesconto_final' with the results
    df_estoque_pordesconto_bruto = pd.DataFrame.from_dict(
        resultado_valor_estoque_faixa_desconto
    )
    df_relger_estoque_pordesconto_final = df_estoque_pordesconto_bruto.T
    df_relger_estoque_pordesconto_final.columns = (
        df_relger_estoque_pordesconto_final.iloc[0]
    )
    df_relger_estoque_pordesconto_final = (
        df_relger_estoque_pordesconto_final.iloc[1:].copy()
    )
    df_relger_estoque_pordesconto_final = (
        df_relger_estoque_pordesconto_final.reset_index(drop=True)
    )

    # In[9]: REL GER: Calculate valores de frete e desconto do pedido

    # Calcular valor total de frete dos pedidos aprovados
    resultado_valor_frete_total = df_ecco_ped["frete"].sum()

    # Calcular representidade do valor de frete sobre valor total de vendas
    resultado_porcentagem_frete_total = (
        resultado_valor_frete_total / resultado_valor_vendas_total
    )

    # Calcular valor total de desconto dos pedidos aprovados
    resultado_valor_desconto_pedido_total = df_ecco_ped["desconto"].sum()

    # Calcular representidade do valor de desconto sobre valor total de vendas
    resultado_porcentagem_desconto_pedido_total = (
        resultado_valor_desconto_pedido_total / resultado_valor_vendas_total
    )

    # Criar DF final de informações complementares de pedidos
    df_relger_informacoes_total = pd.DataFrame.from_dict(
        {
            "VALOR FRETE COBRADO TOTAL": [resultado_valor_frete_total],
            "% FRETE": [resultado_porcentagem_frete_total],
            "VALOR DESCONTO PEDIDO TOTAL": [
                resultado_valor_desconto_pedido_total
            ],
            "% DESCONTO PEDIDO": [resultado_porcentagem_desconto_pedido_total],
        }
    )

    # In[10]: Montar DF final de relger com todos os dados

    # Juntar todos os dfs
    df_relger_ecco_final = pd.concat(
        [
            df_relger_vendas_total,
            df_relger_estoque_total,
            df_relger_informacoes_total,
            df_relger_vendas_pordesconto_final,
            df_relger_estoque_pordesconto_final,
        ],
        axis=1,
    )

    # In[11]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    # email: str = os.environ.get("supabase_email")
    # password: str = os.environ.get("supabase_password")

    # data = Client.sign_in(
    #     {"email": email, "password": password}
    # )

    dic_relger_ecco_final = df_relger_ecco_final.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"df_relger_ecco_{client}")
            .upsert(dic_relger_ecco_final)
            .execute()
        )

    except Exception as exception:
        print(exception)
