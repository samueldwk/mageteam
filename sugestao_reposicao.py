# SUGESTÃO DE REPOSIÇÃO AUTOMÁTICA POR TAMANHO

import requests
import pandas as pd
import dotenv
import os
import time
import datetime
import date_functions_retroativo as datef
import gspread


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 9, 20).date()

(
    datatxt,
    dataname1,
    datasql,
    datatxt2,
    dataname2,
    datatxt3,
    dataname3,
    dataname4,
) = datef.dates(d1)


# CLIENT LIST
c_list = [
    # "alanis",
    "basicler",
    # "dadri",
    # "french",opaa ai
    # "haut",
    # "infini",
    # "kle",
    # "morina",
    # "mun",
    # "muna",
    # "nobu",
    # "othergirls",
    # "rery",
    # "talgui",
    # "una",
    # "uniquechic",
]

# c_list = ["alanis"]

# DICIONÁRIO DE NOMES

dic_nomes = {
    "alanis": "Alanis",
    "basicler": "Mixxon/Basicler",
    "dadri": "Dadri",
    "french": "French",
    "haut": "Haut",
    "infini": "Infini",
    "kle": "Kle",
    "luvic": "Luvic",
    "morina": "Morina",
    "mun": "Mun",
    "muna": "Muna",
    "nobu": "Nobu",
    "othergirls": "Other Girls",
    "paconcept": "P.A Concept",
    "rery": "Rery",
    "talgui": "Talgui",
    "una": "Una",
    "uniquechic": "Unique Chic",
}

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

    # In[2]: CALL PRODUCTS FROM EACH ORDER AND MAKE PEDIDOS X PRODUTOS
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

    ### COLOCAR DESCONTO DO PRODUTO

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

    ### DETERMINAR A FAIXA DE DESCONTO DO PRODUTO

    # Trazer precoLancamentoProduto para tabela de vendas produto
    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_produto,
        how="left",
        right_on="idProduto",
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
            "codigo": "codigoProduto",
        }
    )

    # Columns to keep na df de pedidos produto
    columns_to_keep = [
        # "idPedido",
        # "idProduto",
        "QuantidadeVenda",
        # "ValorVendaUnit",
        "PorcentagemDescontoProduto",
        # "FaixaDescontoProduto",
        "NomeProduto",
        # "ValorDescontoPedido",
        "codigoProduto",
        # "DataVendaPedido",
    ]

    df_ecco_ped_prod = df_ecco_ped_prod[columns_to_keep]

    # Colocar coluna mage_cliente
    df_ecco_ped_prod["mage_cliente"] = client

    # Agrupar vendas por produto
    df_vendas_produto = (
        df_ecco_ped_prod.groupby("codigoProduto")["QuantidadeVenda"]
        .sum()
        .reset_index()
    )

    df_vendas_produto = (
        df_ecco_ped_prod.groupby("codigoProduto")
        .agg(
            venda_quantidade_total=("QuantidadeVenda", "sum"),
            desconto_produto_maximo=("PorcentagemDescontoProduto", "max"),
            NomeProduto=("NomeProduto", "last"),
        )
        .reset_index()
    )

    # Calcular média de venda diária
    df_vendas_produto["venda_media_diario"] = (
        df_vendas_produto["venda_quantidade_total"] / 3
    )

    # In[3]: Eccosys API: GET Listar quantidades em estoque

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

    # In[4]: Juntar vendas com estoque por produto e falar qual produto vai acabar em menos de 7 dias

    df_vendas_estoque = df_vendas_produto.merge(
        df_ecco_estoque_limpo[["codigoProduto", "estoqueDisponivel"]],
        on="codigoProduto",
        how="left",  # Use 'left' to keep all rows from df_vendas_produto
    )

    # Calcular cobertura de dias
    df_vendas_estoque["cobertura_dias"] = (
        df_vendas_estoque["estoqueDisponivel"]
        / df_vendas_estoque["venda_media_diario"]
    )

    # Drop produtos com cobertura maior que 3 dias (sábado, domingo e segunda feira)
    df_vendas_estoque = df_vendas_estoque[
        df_vendas_estoque["cobertura_dias"] <= 3
    ]

    # Drop produtos com venda maior ou igual a 2 em 3 dias
    df_vendas_estoque = df_vendas_estoque[
        df_vendas_estoque["venda_quantidade_total"] >= 2
    ]

    # Calcular sugestao de reposicao (cobertura para 4 dias)
    df_vendas_estoque["qtd_sugestao_reposicao"] = (
        df_vendas_estoque["venda_media_diario"] * 4
    ) - df_vendas_estoque["estoqueDisponivel"]

    # Drop produtos com venda maior ou igual a 2 em 3 dias
    df_vendas_estoque = df_vendas_estoque[
        df_vendas_estoque["qtd_sugestao_reposicao"] >= 1
    ]

    df_vendas_estoque["data_requisicao"] = dataname1

    columns_to_keep_final = [
        "data_requisicao",
        "codigoProduto",
        "NomeProduto",
        "desconto_produto_maximo",
        "venda_quantidade_total",
        "venda_media_diario",
        "estoqueDisponivel",
        "cobertura_dias",
        "qtd_sugestao_reposicao",
    ]

    df_vendas_estoque = df_vendas_estoque[columns_to_keep_final]

    # Creating df_vendas_estoque_newin where desconto_produto_maximo is 0
    df_vendas_estoque_newin = df_vendas_estoque[
        df_vendas_estoque["desconto_produto_maximo"] == 0
    ]

    # Creating df_vendas_estoque_sale where desconto_produto_maximo is greater than 0
    df_vendas_estoque_sale = df_vendas_estoque[
        df_vendas_estoque["desconto_produto_maximo"] > 0
    ]

    # %% UPDATE GOOGLE SHEETS

    # UPDATE NEW IN

    gc = gspread.oauth()

    sh = gc.open(f"{dic_nomes[client]} - Relatório Reposição").worksheet(
        "NEW IN"
    )

    list_final_newin = df_vendas_estoque_newin.values.tolist()

    print(f"{dic_nomes[client]} - Relatório Reposição - NEW IN - {dataname1}")

    sh.append_rows(list_final_newin, table_range="A1")

    # UPDATE SALE

    gc = gspread.oauth()

    sh = gc.open(f"{dic_nomes[client]} - Relatório Reposição").worksheet(
        "SALE"
    )

    list_final_sale = df_vendas_estoque_sale.values.tolist()

    print(f"{dic_nomes[client]} - Relatório Reposição - SALE - {dataname1}")

    sh.append_rows(list_final_sale, table_range="A1")
