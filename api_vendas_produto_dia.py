# ANALISE DE VENDAS DIARIA POR PRODUTO E ESTOQUE FINAL

import requests
import pandas as pd
import dotenv
import os
import time
import datetime

dotenv.load_dotenv()

# DATE FUCTIONS
d1 = datetime.datetime(2024, 10, 7).date()  # SELECT DATE TO
d2 = datetime.datetime(2024, 9, 29).date()  # SELECT DATE FROM

dataname1 = str(d1)
dataname2 = str(d2)

# CLIENT LIST
c_list = ["alanis"]

# API HEADER
payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: GET LIST OF ORDERS

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={dataname2}&$toDate={dataname1}&$offset=0&$count=50000&$dataConsiderada=data"

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

    ### COLOCAR DESCONTO DO PRODUTO / VERIFICAR O CUSTO DO PRODUTO E ARRUMAR SE PRECISO

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
        "preco",
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
            "preco": "precoAtualProduto",
            "precoCusto": "precoCustoProdutoUnit",
        },
        inplace=True,
    )

    # Converter tipo de valores
    columns_to_convert = [
        "precoLancamentoProduto",
        "precoCustoProdutoUnit",
        "precoAtualProduto",
    ]

    df_ecco_produto[columns_to_convert] = df_ecco_produto[
        columns_to_convert
    ].astype(float)

    # VERIFICAR SE O PREÇO DE LANÇAMENTO ESTÁ CERTO E ARRUMAR SE NECESSÁRIO

    # Replace 0 values in 'precoLancamentoProduto' with corresponding values from 'precoAtualProduto'
    df_ecco_produto["precoLancamentoProduto"] = df_ecco_produto.apply(
        lambda row: row["precoAtualProduto"]
        if row["precoLancamentoProduto"] == 0
        else row["precoLancamentoProduto"],
        axis=1,
    )

    # VERIFICAR SE O CUSTO É RAZOÁVEL E ARRUMAR SE NECESSÁRIO

    # Criar coluna grupo_produto para depois calcular custo médio por grupo e ser usado para corrigir custos errados
    df_ecco_produto["nomeProduto"] = df_ecco_produto["nomeProduto"].str.strip()
    df_ecco_produto["grupo_produto"] = (
        df_ecco_produto["nomeProduto"].str.split(" ").str[0]
    )

    # Criar uma df apenas com produtos pai e seus custos (poder ser que tenha filhos como pais)
    df_ecco_produto_pai = df_ecco_produto[
        df_ecco_produto["idProdutoMaster"] == "0"
    ].copy()

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

    # Criar uma tabela de custo médio por grupo de produto para ser usado caso o custo do produto não seja razoável

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

    # Verificar se o custo do produto pai é razoável e arrumar custos e criar uma lista de produtos com custo errado

    df_ecco_produto_custo_arrumado["mkp_lancamento_produto"] = (
        df_ecco_produto_custo_arrumado["precoLancamentoProduto"]
        / df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"]
    )

    # Step 1: Identify wrong costs based on both conditions
    mask_wrong_costs = (
        (df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"] < 1)
        | (df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"] > 1000)
    ) | (
        (df_ecco_produto_custo_arrumado["mkp_lancamento_produto"] < 0.5)
        | (df_ecco_produto_custo_arrumado["mkp_lancamento_produto"] > 10)
    )

    # Step 2: List products with wrong costs
    df_produtos_cadastro_preco_errado = df_ecco_produto_custo_arrumado[
        mask_wrong_costs
    ]

    # Step 3: Substituting wrong costs by group average
    # Assuming df_grupo_produto_custo_media has 'grupo_produto_x' and 'precoCustoProdutoPaiUnit'
    # Merging the average cost dataframe with the original df on 'grupo_produto'
    df_ecco_produto_custo_arrumado_final = (
        df_ecco_produto_custo_arrumado.merge(
            df_grupo_produto_custo_media[
                ["grupo_produto_x", "precoCustoProdutoPaiUnit"]
            ],
            on="grupo_produto_x",
            suffixes=("", "_media"),
        )
    )

    # Step 4: Replace wrong costs with group average where cost is invalid
    df_ecco_produto_custo_arrumado_final.loc[
        mask_wrong_costs, "precoCustoProdutoPaiUnit"
    ] = df_ecco_produto_custo_arrumado_final.loc[
        mask_wrong_costs, "precoCustoProdutoPaiUnit_media"
    ]

    # Step 5: Drop the auxiliary 'columns
    df_ecco_produto_custo_arrumado_final.drop(
        columns=[
            "precoCustoProdutoUnit",
            "nomeProduto_x",
            "grupo_produto_x",
            "idProduto_y",
            "idProdutoMaster_y",
            "precoLancamentoProdutoPai",
            "nomeProduto_y",
            "grupo_produto_y",
            "mkp_lancamento_produto",
            "precoCustoProdutoPaiUnit_media",
        ],
        inplace=True,
    )

    ### DETERMINAR A FAIXA DE DESCONTO DO PRODUTO E TRAZER CUSTO CERTO

    # Trazer precoLancamentoProduto para tabela de vendas produto
    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_produto_custo_arrumado_final,
        how="left",
        right_on="idProduto_x",
        left_on="idProduto",
    )

    # Calcular PorcentagemDescontoProduto
    df_ecco_ped_prod["PorcentagemDescontoProduto"] = 1 - (
        df_ecco_ped_prod["valor"] / df_ecco_ped_prod["precoLancamentoProduto"]
    )

    try:
        df_ecco_ped_prod.fillna(0, inplace=True)
    except TypeError:
        print(f"{client} - df_ecco_ped_prod: Não contém NAN")

    # Arredondar o desconto de produto
    precision = 2
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

    # Columns to keep na df de pedidos produto
    columns_to_keep = [
        "idPedido",
        "idProduto",
        "QuantidadeVenda",
        "precoCustoProdutoPaiUnit",
        "ValorVendaUnit",
        "PorcentagemDescontoProduto",
        "FaixaDescontoProduto",
        "NomeProduto",
        "ValorDescontoPedido",
        "CodigoProduto",
        "DataVendaPedido",
    ]

    df_ecco_ped_prod = df_ecco_ped_prod[columns_to_keep]

    # Colocar coluna mage_cliente
    df_ecco_ped_prod["mage_cliente"] = client

    # In[1]: Eccosys API: GET Listar quantidades em estoque

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

    df_ecco_estoque_limpo["estoqueReal"] = df_ecco_estoque_limpo[
        "estoqueReal"
    ].apply(lambda x: x if x > 0 else 0)

    df_ecco_estoque_limpo["estoqueDisponivel"] = df_ecco_estoque_limpo[
        "estoqueDisponivel"
    ].apply(lambda x: x if x > 0 else 0)

    # Colocar coluna de data estoque
    df_ecco_estoque_limpo["data"] = dataname1

    # Tranformar formato coluna estoque para int
    columns_to_convert = ["estoqueDisponivel", "estoqueReal"]

    df_ecco_estoque_limpo[columns_to_convert] = df_ecco_estoque_limpo[
        columns_to_convert
    ].astype(int)

    # Renomear os nomes das colunas
    df_ecco_estoque_limpo.rename(
        columns={"codigo": "codigoProduto", "nome": "nomeProduto"},
        inplace=True,
    )

    # In[2]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data"

    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_prod = response_prod.json()
    df_ecco_prod = pd.DataFrame.from_dict(dic_ecco_prod)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "preco",
        "precoDe",
        "precoCusto",
        "situacao",
    ]

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # Renomear os nomes das colunas
    df_ecco_prod.rename(
        columns={
            "id": "idProduto",
            "preco": "precoProduto",
            "precoDe": "precoLancamentoProduto",
            "precoCusto": "precoCustoProdutoUnit",
            "situacao": "statusProduto",
        },
        inplace=True,
    )

    # Tranformar formato coluna idProduto para int
    columns_to_convert = ["idProduto"]
    df_ecco_prod[columns_to_convert] = df_ecco_prod[columns_to_convert].astype(
        int
    )

    # Trazer informação de produto para estoque
    df_ecco_estoque_final = df_ecco_estoque_limpo.merge(
        df_ecco_prod, on="idProduto", how="left"
    )

    # CONVERT COLUMNS TYPE
    columns_to_convert = [
        "precoLancamentoProduto",
        "precoProduto",
        "precoCustoProdutoUnit",
    ]

    df_ecco_estoque_final[columns_to_convert] = df_ecco_estoque_final[
        columns_to_convert
    ].astype(float)

    # VERIFICAR SE O PREÇO DE LANÇAMENTO ESTÁ CERTO E ARRUMAR SE NECESSÁRIO

    # Replace 0 values in 'precoLancamentoProduto' with corresponding values from 'precoAtualProduto'
    df_ecco_estoque_final[
        "precoLancamentoProduto"
    ] = df_ecco_estoque_final.apply(
        lambda row: row["precoProduto"]
        if row["precoLancamentoProduto"] == 0
        else row["precoLancamentoProduto"],
        axis=1,
    )

    # Calcular PorcentagemDescontoProduto
    df_ecco_estoque_final["PorcentagemDescontoProduto"] = 1 - (
        df_ecco_estoque_final["precoProduto"]
        / df_ecco_estoque_final["precoLancamentoProduto"]
    )

    df_ecco_estoque_final.fillna(0, inplace=True)

    # Arredondar o desconto de produto
    precision = 2
    df_ecco_estoque_final.loc[
        :, "PorcentagemDescontoProduto"
    ] = df_ecco_estoque_final["PorcentagemDescontoProduto"].round(precision)

    # Criar faixas de desconto de produto
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "E: <= 0%",
        "E: > 0% and <= 25%",
        "E: > 25% and <= 45%",
        "E: > 45% and <= 60%",
        "E: > 60%",
    ]

    # Adicionar nova coluna com faixa de desconto de produto
    df_ecco_estoque_final.loc[:, "FaixaDescontoProduto"] = pd.cut(
        df_ecco_estoque_final["PorcentagemDescontoProduto"],
        bins=bins,
        labels=labels,
    )

    # Drop columns que nao precisa mandar para database
    df_ecco_estoque_final = df_ecco_estoque_final.drop(
        columns=["precoLancamentoProduto"]
    )

    # Colocar coluna mage_cliente
    df_ecco_estoque_final["mage_cliente"] = client

    # In[3]: Define the directory where you want to save the file
    directory = r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"  # Update this to the desired directory

    # SALVAR VENDAS

    file_name = f"{client}_vendas.xlsx"

    # Define the full file path
    file_path = os.path.join(directory, file_name)

    # Save DataFrame to Excel file
    df_ecco_ped_prod.to_excel(file_path, index=False)

    # SALVAR ESTOQUE

    file_name = f"{client}_estoque.xlsx"

    # Define the full file path
    file_path = os.path.join(directory, file_name)

    # Save DataFrame to Excel file
    df_ecco_estoque_final.to_excel(file_path, index=False)
