# api_eccosys_vendas_v1 (3 dias de venda anterior) por pedido e por produto gitactions

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
    # "dadri",
    "french",
    # "infini",
    # "mun",
    # "othergirls",
    "talgui",
    # "una",
]

# c_list = ["french"]

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

    df_ecco_ped.fillna("", inplace=True)

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

    # df_ecco_ped.dtypes

    # Colocar coluna mage_cliente
    df_ecco_ped["mage_cliente"] = client

    # In[2]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_ped = df_ecco_ped.to_dict(orient="records")

    try:
        response = (
            supabase.table("mage_eccosys_pedidos_v1")
            .upsert(dic_ecco_ped)
            .execute()
        )

    except Exception as exception:
        print(f"{client}: {exception}")

    print(f"{client}: api_eccosys_pedidos_v1 (OK)")

    # In[3]: CALL PRODUCTS FROM EACH ORDER AND MAKE PEDIDOS X PRODUTOS
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

    # COLOCAR DESCONTO DO PRODUTO / VERIFICAR O CUSTO DO PRODUTO E ARRUMAR SE PRECISO

    # Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$opcEcommerce=S"

    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_produto = response_prod.json()
    df_ecco_produto = pd.DataFrame.from_dict(dic_ecco_produto)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "idProdutoMaster",
        "precoDe",
        "precoCusto",
        "nome",
    ]

    df_ecco_produto = df_ecco_produto[columns_to_keep]

    # Renomear os nomes das colunas
    df_ecco_produto.rename(
        columns={
            "id": "idProduto",
            "nome": "nomeProduto",
            "precoDe": "precoLancamentoProduto",
            "precoCusto": "precoCustoProdutoUnit",
        },
        inplace=True,
    )

    # Converter tipo de valores
    columns_to_convert = ["precoLancamentoProduto", "precoCustoProdutoUnit"]

    df_ecco_produto[columns_to_convert] = df_ecco_produto[
        columns_to_convert
    ].astype(float)

    ### VERIFICAR SE O CUSTO É RAZOÁVEL E ARRUMAR SE NECESSÁRIO

    # Criar coluna grupo_produto para depois calcular custo médio por grupo e ser usado para corrigir custos errados
    df_ecco_produto["nomeProduto"] = df_ecco_produto["nomeProduto"].str.strip()
    df_ecco_produto["grupo_produto"] = (
        df_ecco_produto["nomeProduto"].str.split(" ").str[0]
    )

    # Criar uma df apenas com produtos pai e seus custos (poder ser que tenha filhos como pais)
    df_ecco_produto_pai = df_ecco_produto[
        df_ecco_produto["idProdutoMaster"] == "0"
    ]

    # Renomear colunas da df_ecco_produto_pai
    df_ecco_produto_pai.rename(
        columns={
            "id": "idProduto",
            "precoLancamentoProduto": "precoLancamentoProdutoPai",
            "precoCustoProdutoUnit": "precoCustoProdutoPaiUnit",
        },
        inplace=True,
    )

    # Trazer o custo do produto pai pelo campo idProdutoMaster
    df_ecco_produto_custo_arrumado = df_ecco_produto.merge(
        df_ecco_produto_pai,
        how="left",
        left_on="idProdutoMaster",
        right_on="idProduto",
    )

    # Replace NaN values in 'precoCustoProdutoPaiUnit' with values from 'precoCustoProdutoPaiUnit_media'
    df_ecco_produto_custo_arrumado[
        "precoCustoProdutoPaiUnit"
    ] = df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"].fillna(
        df_ecco_produto_custo_arrumado["precoCustoProdutoUnit"]
    )

    ## Criar uma tabela de custo médio por grupo de produto para ser usado caso o custo do produto não seja razoável

    columns_to_keep = [
        "idProduto_x",
        "idProdutoMaster_x",
        "nomeProduto_x",
        "precoCustoProdutoPaiUnit",
        "grupo_produto_x",
    ]
    df_ecco_produto_custo_grupo = df_ecco_produto_custo_arrumado[
        columns_to_keep
    ]

    # Manter apenas linhas onde custo é maior que 1 ou menor que 1000
    df_ecco_produto_custo_grupo = df_ecco_produto_custo_grupo[
        (df_ecco_produto_custo_grupo["precoCustoProdutoPaiUnit"] >= 1)
        & (df_ecco_produto_custo_grupo["precoCustoProdutoPaiUnit"] <= 1000)
    ]
    df_ecco_produto_custo_grupo = df_ecco_produto_custo_grupo.dropna()

    # Calcular média de custo por grupo de produto e criar uma df com essas médias
    df_grupo_produto_custo_media = (
        df_ecco_produto_custo_grupo.groupby("grupo_produto_x")[
            "precoCustoProdutoPaiUnit"
        ]
        .mean()
        .reset_index()
    )

    ## Verificar se o custo do produto pai é razoável e arrumar custos e criar uma lista de produtos com custo errado

    # Step 1: Identify the wrong costs (smaller than 1 or greater than 1000)
    mask_wrong_costs = (
        df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"] < 1
    ) | (df["precoCustoProdutoPaiUnit"] > 1000)

    # Step 2: List of products with wrong costs
    products_with_wrong_costs = df[mask_wrong_costs]

    # Step 3: Substituting wrong costs by group average

    # Assuming df_grupo_produto_custo_media has 'grupo_produto' and 'average_cost'
    # Merging the average cost dataframe with the original df on 'grupo_produto'
    df = df.merge(
        df_grupo_produto_custo_media[
            ["grupo_produto", "precoCustoProdutoPaiUnit"]
        ],
        on="grupo_produto",
        suffixes=("", "_media"),
    )

    # Step 4: Replace wrong costs with group average where cost is invalid
    df.loc[mask_wrong_costs, "precoCustoProdutoPaiUnit"] = df.loc[
        mask_wrong_costs, "precoCustoProdutoPaiUnit_media"
    ]

    # Step 5: Drop the auxiliary 'precoCustoProdutoPaiUnit_media' column
    df.drop(columns=["precoCustoProdutoPaiUnit_media"], inplace=True)

    # The DataFrame now has corrected costs, and products_with_wrong_costs has the products with wrong costs.

    ### DETERMINAR A FAIXA DE DESCONTO DO PRODUTO

    # Trazer precoLancamentoProduto para tabela de vendas produto
    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_produto, how="left", on="idProduto"
    )

    # Calcular PorcentagemDescontoProduto
    df_ecco_ped_prod["PorcentagemDescontoProduto"] = 1 - (
        df_ecco_ped_prod["valor"] / df_ecco_ped_prod["precoLancamentoProduto"]
    )

    df_ecco_ped_prod.fillna(0, inplace=True)

    # Arredondar o desconto de produto
    precision = 5
    df_ecco_ped_prod.loc[:, "PorcentagemDescontoProduto"] = df_ecco_ped_prod[
        "PorcentagemDescontoProduto"
    ].round(precision)

    # Criar faixas de desconto de produto
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
    ]

    # Adicionar nova coluna com faixa de desconto de produto
    df_ecco_ped_prod.loc[:, "FaixaDescontoProduto"] = pd.cut(
        df_ecco_ped_prod["PorcentagemDescontoProduto"],
        bins=bins,
        labels=labels,
    )

    # Columns to keep na df de pedidos produto
    columns_to_keep = [
        "idVenda",
        "idProduto",
        "quantidade",
        "precoCustoProdutoUnit",
        "valor",
        "PorcentagemDescontoProduto",
        "FaixaDescontoProduto",
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

    # Colocar coluna mage_cliente
    df_ecco_ped_prod["mage_cliente"] = client

    # In[4]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_ped_prod = df_ecco_ped_prod.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"mage_eccosys_vendas_produto_v1")
            .upsert(dic_ecco_ped_prod)
            .execute()
        )

    except Exception as exception:
        print(f"{client}: {exception}")

    print(f"{client}: api_eccosys_vendas_produto_v1 (OK)")
