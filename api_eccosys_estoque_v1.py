# api_eccosys_estoque_v1 gitactions

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time
import math
from pushover_notification import send_pushover_notification


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

# CLIENT LIST
c_list = [
    "alanis",
    "basicler",
    # "dadri",
    "french",
    "haut",
    # "infini",
    "kle",
    "morina",
    "mun",
    "muna",
    # "nobu",
    "othergirls",
    "rery",
    "talgui",
    "tob",
    "paconcept",
    "una",
    "uniquechic",
]

# c_list = ["tob"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

# supabase_admin_user = os.environ.get("supabase_admin_user")
# supabase_admin_password = os.environ.get("supabase_admin_password")

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# # Autentificar usuário
# auth_response = supabase.auth.sign_in_with_password(
#     {"email": supabase_admin_user, "password": supabase_admin_password}
# )

# print(auth_response)

time.sleep(5)

# ECCOSYS API HEADER

payload = {}
files = {}

# transformar tudo que esta aqui dentro do for em função
for client in c_list:
    try:
        headers = {
            "Authorization": os.getenv(f"ecco_aut_{client}"),
            "Content-Type": "application/json;charset=utf-8",
        }

        # In[1]: Eccosys API: GET Listar quantidades em estoque

        # colocar API em função
        url_estoque = "https://empresa.eccosys.com.br/api/estoques?&$offset=0&$count=10000000"

        response_estoque = requests.request(
            "GET", url_estoque, headers=headers, data=payload, files=files
        )

        contador = 0

        while response_estoque.status_code != 200:
            print(f"{client}: Status_code response = {response_estoque}")

            time.sleep(150)

            contador = contador + 1

            response_estoque = requests.request(
                "GET", url_estoque, headers=headers, data=payload, files=files
            )

            if contador == 3:
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

        # Remover do dataframe todos produtos que tem estoque disp e real = 0

        df_ecco_estoque_limpo = df_ecco_estoque_limpo[
            ~(
                (df_ecco_estoque_limpo["estoqueReal"] == 0)
                & (df_ecco_estoque_limpo["estoqueDisponivel"] == 0)
            )
        ]

        # Colocar coluna de data estoque
        df_ecco_estoque_limpo["data"] = dataname

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

        # colocar api em funcao
        url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data"

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
            "situacao",
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
                "situacao": "statusProduto",
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
            | (
                df_ecco_produto_custo_arrumado["precoCustoProdutoPaiUnit"]
                > 1000
            )
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

        # Tranformar formato coluna idProduto para int
        columns_to_convert = ["idProduto_x"]
        df_ecco_produto_custo_arrumado_final[
            columns_to_convert
        ] = df_ecco_produto_custo_arrumado_final[columns_to_convert].astype(
            int
        )

        # In[3]: Juntar estoque com produto

        # Trazer informação de produto para estoque
        df_ecco_estoque_final = df_ecco_estoque_limpo.merge(
            df_ecco_produto_custo_arrumado_final,
            left_on="idProduto",
            right_on="idProduto_x",
            how="left",
        )

        # CONVERT COLUMNS TYPE
        columns_to_convert = [
            "precoLancamentoProduto",
            "precoAtualProduto_x",
            "precoCustoProdutoPaiUnit",
        ]

        df_ecco_estoque_final[columns_to_convert] = df_ecco_estoque_final[
            columns_to_convert
        ].astype(float)

        # Calcular PorcentagemDescontoProduto
        df_ecco_estoque_final["PorcentagemDescontoProduto"] = 1 - (
            df_ecco_estoque_final["precoAtualProduto_x"]
            / df_ecco_estoque_final["precoLancamentoProduto"]
        )

        df_ecco_estoque_final.fillna(0, inplace=True)

        # Arredondar o desconto de produto
        precision = 2
        df_ecco_estoque_final.loc[
            :, "PorcentagemDescontoProduto"
        ] = df_ecco_estoque_final["PorcentagemDescontoProduto"].round(
            precision
        )

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

        # Renomear os nomes das colunas
        df_ecco_estoque_final.rename(
            columns={
                "precoAtualProduto_x": "precoProduto",
                "precoCustoProdutoPaiUnit": "precoCustoProdutoUnit",
                "statusProduto_x": "statusProduto",
            },
            inplace=True,
        )

        # COLUMNS TO KEEP para dataframe final que vai ir para o banco
        columns_to_keep = [
            "estoqueReal",
            "estoqueDisponivel",
            "codigoProduto",
            "nomeProduto",
            "idProduto",
            "data",
            "precoProduto",
            "precoCustoProdutoUnit",
            "statusProduto",
            "PorcentagemDescontoProduto",
            "FaixaDescontoProduto",
        ]

        df_ecco_estoque_final = df_ecco_estoque_final[columns_to_keep]

        # Colocar coluna mage_cliente
        df_ecco_estoque_final["mage_cliente"] = client

        # df_ecco_estoque_final.dtypes

        # In[4]: Enviar informações para DB

        dic_ecco_estoque_final = df_ecco_estoque_final.to_dict(
            orient="records"
        )

        # Set batch size (e.g., 10000 records per batch)
        batch_size = 10000

        # Calculate how many batches we need
        num_batches = math.ceil(len(dic_ecco_estoque_final) / batch_size)

        # Dictionary to hold the DataFrames, where keys are the batch names
        batches_df = {}

        # Loop through the data and upsert it in batches
        for i in range(num_batches):
            # Create a batch by slicing the dictionary
            batch = dic_ecco_estoque_final[
                i * batch_size : (i + 1) * batch_size
            ]

            try:
                # Create a unique name for each batch DataFrame
                batch_name = f"df_batch_{i + 1}"

                # Convert the batch into a DataFrame and store it in the dictionary
                batches_df[batch_name] = pd.DataFrame(batch)

                # Upsert this batch into the database
                response = (
                    supabase.table("mage_eccosys_estoque_v1")
                    .upsert(batch, returning="minimal")
                    .execute()
                )

                print(
                    f"{client}: api_eccosys_estoque_v1 Batch {i + 1}/{num_batches} inserted successfully."
                )
            except Exception as exception:
                print(
                    f"*****ERRO: {client}: api_eccosys_estoque_v1 Batch {i + 1} failed: {exception}"
                )

    except Exception as e:
        print(
            f"*****ERRO: {client} | api_eccosys_estoque_v1 | {e} [{dataname}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | api_eccosys_estoque_v1 | {e} [{dataname}]"
        )
