# api_eccosys_vendas_v3 (3 dias de venda anterior) por pedido e por produto gitactions
# Atualização em relação a V.1 é como lidar com custos errados, em relação a V.2 é que eu apenas trago itens de pedidos que ainda não estão gravados no banco

import requests
import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os
import time
from pushover_notification import send_pushover_notification
import numpy as np


dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime(2024, 11, 2).date()  # SELECT DATE

datatxt, dataname1, datasql, dataname2, dataname3, dataname4 = datef.dates(d1)
dataname4_date_format = datetime.strptime(dataname4, "%Y-%m-%d").date()
dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
dataname2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()


# CLIENT LIST

c_list = [
    # "alanis",
    # "basicler",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    # "morina",
    # "mun",
    # "muna",
    # "nobu",
    # "othergirls",
    # "rery",
    "talgui",
    # "tob",
    # "paconcept",
    # "una",
    # "uniquechic",
]

# c_list = ["french"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# ECCOSYS API HEADER

payload = {}
files = {}


for client in c_list:
    try:
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

        df_ecco_ped["desconto"] = df_ecco_ped["desconto"].str.replace(".", "")

        df_ecco_ped["desconto"] = df_ecco_ped["desconto"].str.replace(",", ".")

        columns_to_convert = [
            "totalProdutos",
            "desconto",
            "totalVenda",
            "frete",
        ]
        df_ecco_ped[columns_to_convert] = df_ecco_ped[
            columns_to_convert
        ].astype(float)

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
            list(dic_status_name.items()),
            columns=["Status Code", "Status Name"],
        )

        # MERGE PEDIDOS WITH STATUS NAME ON SITUACAO

        df_ecco_ped = df_ecco_ped.merge(
            df_status_name, left_on="situacao", right_on="Status Code"
        )

        # COLUMNS TO KEEP

        columns_to_keep = [
            "id",
            "idContato",
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

        dic_ecco_ped = df_ecco_ped.to_dict(orient="records")

        try:
            response = (
                supabase.table("mage_eccosys_pedidos_v1")
                .upsert(dic_ecco_ped, returning="minimal")
                .execute()
            )

        except Exception as exception:
            print(f"{client}: {exception}")

        print(f"{client}: api_eccosys_pedidos_v1")

        # In[3]: Trazer da db quais pedidos eu já tenho gravado para nao precisar pegar itens de todos pedidos

        def get_row_by_two_columns(
            client, dataname3_date_format, dataname_date_format
        ):
            try:
                supabase: Client = create_client(url, key)

                response = (
                    supabase.table("mage_eccosys_vendas_produto_v1")
                    .select("idPedido")
                    .gte("DataVendaPedido", dataname3_date_format)
                    .lte("DataVendaPedido", dataname_date_format)
                    .eq("mage_cliente", f"{client}")
                    .execute()
                )

                # Check if a matching row exists
                if response.data:
                    return response.data  # Returns all matching rows
                else:
                    return None
            except Exception as e:
                # Print the error to debug more effectively
                print(f"Error fetching data for client {client}: {e}")
                raise

        try:
            rows = get_row_by_two_columns(
                client, dataname3_date_format, dataname1_date_format
            )
            if rows:
                df_orders_ids_database = pd.DataFrame(rows)
            else:
                print(f"No matching row found for client: {client}")
                df_orders_ids_database = pd.DataFrame(columns=["idPedido"])

        except Exception as e:
            print(e)

        # In[3]: CALL PRODUCTS FROM EACH ORDER AND MAKE PEDIDOS X PRODUTOS
        df_order_ids = df_ecco_ped["idVenda"]

        df_order_ids = df_order_ids.to_frame(name="idVenda")
        df_order_ids = df_order_ids.astype(str)

        df_orders_ids_database["idPedido"] = df_orders_ids_database[
            "idPedido"
        ].astype(str)

        # Filtrar apenas orders novos

        # Perform a left merge with an indicator column to identify unmatched rows
        df_merged = df_order_ids.merge(
            df_orders_ids_database,
            left_on="idVenda",
            right_on="idPedido",
            how="left",
            indicator=True,
        )

        # Keep only rows that are in df_order_ids but not in df_orders_ids_database
        df_new_orders_ids = df_merged[df_merged["_merge"] == "left_only"].drop(
            columns="_merge"
        )

        # Check if there are new orders to process
        if df_new_orders_ids.empty:
            print("No new orders to process; finishing code.")

        else:
            df_ecco_ped_prod = pd.DataFrame()

            for order in df_new_orders_ids["idVenda"]:
                url_ped_prod = (
                    f"https://empresa.eccosys.com.br/api/pedidos/{order}/items"
                )

                while True:
                    response_ped_prod = requests.get(
                        url_ped_prod,
                        headers=headers,
                        data=payload,
                        files=files,
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
                        [df_ecco_ped_prod, df_ecco_ped_prod_un],
                        ignore_index=True,
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

            # Base URL for the API
            url_prod = "https://empresa.eccosys.com.br/api/produtos"
            count = 1000  # Number of products per request
            offset = 0  # Start from the first product

            # Initialize an empty list to store product data
            all_products = []

            while True:
                # Build the query string with dynamic offset
                params = {
                    "$offset": offset,
                    "$count": count,
                    "$dataConsiderada": "data",
                    "$situacao": "A",
                    "$opcEcommerce": "S",
                }

                # Make the API request
                response_prod = requests.get(
                    url_prod, params=params, headers=headers
                )

                # Handle errors or end of products
                if response_prod.status_code == 404:
                    print("No more products found. Finishing loop.")
                    break
                elif response_prod.status_code != 200:
                    print(
                        f"Error: {response_prod.status_code} - {response_prod.text}"
                    )
                    break

                # Parse response JSON
                products = response_prod.json()
                # products = data.get("products", [])  # Adjust based on the API's response structure

                # Add products to the list
                all_products.extend(products)

                # Increment offset for the next request
                offset += count

            # Convert the list of products into a DataFrame
            df_ecco_produto = pd.DataFrame(all_products)

            # Show a preview of the DataFrame
            print(f"Total products retrieved: {len(df_ecco_produto)}")

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
            df_ecco_produto["nomeProduto"] = df_ecco_produto[
                "nomeProduto"
            ].str.strip()
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
            ] = df_ecco_produto_custo_arrumado[
                "precoCustoProdutoPaiUnit"
            ].fillna(
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
                & (
                    df_ecco_produto_custo_grupo["precoCustoProdutoPaiUnit"]
                    <= 1000
                )
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
                (
                    df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"]
                    < 1
                )
                | (
                    df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"]
                    > 1000
                )
            ) | (
                (
                    df_ecco_produto_custo_arrumado["mkp_lancamento_produto"]
                    < 0.5
                )
                | (
                    df_ecco_produto_custo_arrumado["mkp_lancamento_produto"]
                    > 10
                )
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
                    how="left",
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
                df_ecco_ped_prod["valor"]
                / df_ecco_ped_prod["precoLancamentoProduto"]
            )

            # Lidar com produtos com preço de lançamento e atual = 0

            # Replace -inf with 0 in a specific column (e.g., column 'A')
            df_ecco_ped_prod["PorcentagemDescontoProduto"] = df_ecco_ped_prod[
                "PorcentagemDescontoProduto"
            ].replace(-np.inf, 0)

            try:
                df_ecco_ped_prod.fillna(0, inplace=True)
            except TypeError:
                print(f"{client} - df_ecco_ped_prod: Não contém NAN")

            # Arredondar o desconto de produto
            precision = 2
            df_ecco_ped_prod.loc[
                :, "PorcentagemDescontoProduto"
            ] = df_ecco_ped_prod["PorcentagemDescontoProduto"].round(precision)

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

            # Group rows por idPedido e idProduto quando são exatamente os mesmos

            # Group by "idPedido" and "idProduto" and aggregate other columns
            df_ecco_ped_prod_final = df_ecco_ped_prod.groupby(
                ["idPedido", "idProduto"], as_index=False
            ).agg(
                {
                    "QuantidadeVenda": "sum",
                    "precoCustoProdutoPaiUnit": "first",
                    "ValorVendaUnit": "first",
                    "PorcentagemDescontoProduto": "first",
                    "FaixaDescontoProduto": "first",
                    "NomeProduto": "first",
                    "ValorDescontoPedido": "sum",
                    "CodigoProduto": "first",
                    "DataVendaPedido": "first",
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

            df_ecco_ped_prod_final = df_ecco_ped_prod_final[columns_to_keep]

            # Colocar coluna mage_cliente
            df_ecco_ped_prod_final["mage_cliente"] = client

            # In[4]: Enviar informações para DB

            dic_ecco_ped_prod = df_ecco_ped_prod_final.to_dict(
                orient="records"
            )

            # print(dic_ecco_ped_prod)
            try:
                response = (
                    supabase.table("mage_eccosys_vendas_produto_v1")
                    .upsert(dic_ecco_ped_prod, returning="minimal")
                    .execute()
                )

            except Exception as exception:
                print(f"*****ERRO: {client}: {exception}")

            print(f"{client}: api_eccosys_vendas_produto_v2")

    except Exception as e:
        print(
            f"*****ERRO: {client} | api_eccosys_vendas_produto_v3 | {e} [{dataname1}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | api_eccosys_vendas_produto_v3 | {e} [{dataname1}]"
        )
