# api_eccosys_produto_v1.1 gitactions
# api que consegue lidar com lista grande de produtos na hora de fazer upsert no database
# oficial desde 22/09/2024

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
    # "alanis",
    # "basicler",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    # "mun",
    # "muna",
    # "nobu",
    # "othergirls",
    # "rery",
    "talgui",
    # "paconcept",
    # "una",
    # "uniquechic",
]

# c_list = ["una"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

supabase_admin_user = os.environ.get("supabase_admin_user")
supabase_admin_password = os.environ.get("supabase_admin_password")

url: str = os.environ.get("SUPABASE_BI_URL")
key: str = os.environ.get("SUPABASE_BI_KEY")
supabase: Client = create_client(url, key)

# Autentificar usuário
auth_response = supabase.auth.sign_in_with_password(
    {"email": supabase_admin_user, "password": supabase_admin_password}
)

time.sleep(5)

# ECCOSYS API HEADER

payload = {}
files = {}

for client in c_list:
    try:
        headers = {
            "Authorization": os.getenv(f"ecco_aut_{client}"),
            "Content-Type": "application/json;charset=utf-8",
        }

        # In[1]: Eccosys API: GET Listar todos os produtos

        url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$situacao=A&$opcEcommerce=S"

        response_prod = requests.request(
            "GET", url_prod, headers=headers, data=payload
        )

        dic_ecco_produto = response_prod.json()
        df_ecco_produto = pd.DataFrame.from_dict(dic_ecco_produto)

        # COLUMNS TO KEEP
        columns_to_keep = [
            "id",
            "nome",
            "codigo",
            "preco",
            "situacao",
            "precoCusto",
            "idProdutoMaster",
            "precoDe",
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

        # Replace NaN values in 'precoCustoProdutoPaiUnit' with values from 'precoCustoProdutoUnit'
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

        #### Manter apenas linhas onde custo é maior que 1 ou menor que 1000

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

        #### Verificar se o custo do produto pai é razoável e arrumar custos e criar uma lista de produtos com custo errado

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
                "grupo_produto_x",
                "idProduto_y",
                "idProdutoMaster_y",
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

        # Renomear os nomes das colunas
        df_ecco_produto_custo_arrumado_final.rename(
            columns={
                "idProduto_x": "idProduto",
                "nomeProduto_x": "nomeProduto",
                "codigo_x": "codigoProduto",
                "precoAtualProduto_x": "precoProduto",
                "statusProduto_x": "statusProduto",
                "precoCustoProdutoPaiUnit": "precoCustoProduto",
                "idProdutoMaster_x": "idProdutoPai",
            },
            inplace=True,
        )

        # COLUMNS TO KEEP para dataframe final que vai ir para o banco
        columns_to_keep = [
            "idProduto",
            "nomeProduto",
            "codigoProduto",
            "precoLancamentoProduto",
            "precoProduto",
            "statusProduto",
            "precoCustoProduto",
            "idProdutoPai",
        ]

        df_ecco_produto_custo_arrumado_final = (
            df_ecco_produto_custo_arrumado_final[columns_to_keep]
        )

        # Colocar coluna de data cadastro
        df_ecco_produto_custo_arrumado_final["data"] = dataname

        # Colocar coluna mage_cliente
        df_ecco_produto_custo_arrumado_final["mage_cliente"] = client

        # In[11]: Enviar informações para DB

        dic_ecco_produto = df_ecco_produto_custo_arrumado_final.to_dict(
            orient="records"
        )

        # Set batch size (e.g., 10000 records per batch)
        batch_size = 10000

        # Calculate how many batches we need
        num_batches = math.ceil(len(dic_ecco_produto) / batch_size)

        # Loop through the data and upsert it in batches
        for i in range(num_batches):
            # Create a batch by slicing the dictionary
            batch = dic_ecco_produto[i * batch_size : (i + 1) * batch_size]

            try:
                # Upsert this batch into the database
                response = (
                    supabase.table("mage_eccosys_produto_v1")
                    .upsert(batch, returning="minimal")
                    .execute()
                )
                print(
                    f"{client}: api_eccosys_produto_v1.1 Batch {i + 1}/{num_batches} inserted successfully."
                )
            except Exception as exception:
                print(
                    f"{client}: api_eccosys_produto_v1.1 Batch {i + 1} failed: {exception}"
                )

    except Exception as e:
        print(
            f"*****ERRO: {client} | api_eccosys_produto_v1.1 | {e} [{dataname}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | api_eccosys_produto_v1.1 | {e} [{dataname}]"
        )
