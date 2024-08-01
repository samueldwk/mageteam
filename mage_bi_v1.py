# mage_bi_v1

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time
from supabase import create_client, Client
import supabase


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt1, dataname1, datasql, dataname2 = datef.dates(d1)

# CLIENT LIST
c_list = [
    # "alanis",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    "mun",
    # "nobu",
    # "othergirls",
    # "rery",
    # "talgui",
    # "paconcept",
    # "una",
    # "uniquechic",
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

    # In[1]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$opcEcommerce=S"

    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_prod = response_prod.json()
    df_ecco_prod = pd.DataFrame.from_dict(dic_ecco_prod)

    # In[2]: Preparar dados para DB mage_bi_produto

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

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # Renomear os nomes das colunas
    df_ecco_prod.rename(
        columns={
            "id": "idProduto",
            "nome": "nomeProduto",
            "codigo": "codigoProduto",
            "preco": "precoProduto",
            "situacao": "statusProduto",
            "precoCusto": "precoCustoProduto",
            "idProdutoMaster": "idProdutoPai",
            "precoDe": "precoLancamentoProduto",
        },
        inplace=True,
    )

    # Colocar coluna de data cadastro
    df_ecco_prod["Data"] = dataname1

    # Tranformar formato coluna idProduto para int
    columns_to_convert = ["idProduto"]
    df_ecco_prod[columns_to_convert] = df_ecco_prod[columns_to_convert].astype(
        int
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

    dic_ecco_estoque_tamanho = response_estoque.json()
    df_ecco_estoque_tamanaho_cru = pd.DataFrame.from_dict(
        dic_ecco_estoque_tamanho
    )

    # In[4]: Preparar dados para DB (mage_bi_estoque_tamanho)

    # SUBSTITUTE ALL NEGATIVE VALUES BY 0
    df_ecco_estoque_tamanho_limpo = df_ecco_estoque_tamanaho_cru.copy()

    df_ecco_estoque_tamanho_limpo[
        "estoqueReal"
    ] = df_ecco_estoque_tamanho_limpo["estoqueReal"].apply(
        lambda x: x if x > 0 else 0
    )

    df_ecco_estoque_tamanho_limpo[
        "estoqueDisponivel"
    ] = df_ecco_estoque_tamanho_limpo["estoqueDisponivel"].apply(
        lambda x: x if x > 0 else 0
    )

    # Colocar coluna de data estoque
    df_ecco_estoque_tamanho_limpo["Data"] = dataname1

    # Tranformar formato coluna estoque para int
    columns_to_convert = ["estoqueDisponivel", "estoqueReal"]

    df_ecco_estoque_tamanho_limpo[
        columns_to_convert
    ] = df_ecco_estoque_tamanho_limpo[columns_to_convert].astype(int)

    # Renomear os nomes das colunas
    df_ecco_estoque_tamanho_limpo.rename(
        columns={"codigo": "codigoProduto", "nome": "nomeProduto"},
        inplace=True,
    )

    # Trazer informação de produto para estoque
    columns_to_keep = [
        "idProduto",
        "precoProduto",
        "statusProduto",
        "idProdutoPai",
    ]

    df_ecco_prod_estoque = df_ecco_prod[columns_to_keep]

    df_ecco_estoque_tamanho_final = df_ecco_estoque_tamanho_limpo.merge(
        df_ecco_prod_estoque, on="idProduto", how="inner"
    )

    # In[5]: Preparar dados para DB (mage_bi_estoque_variante)

    # Agrupar por 'idProdutoPai' e somar 'estoqueReal' e 'estoqueDisponivel'
    df_ecco_estoque_variante = df_ecco_estoque_tamanho_final.groupby(
        "idProdutoPai"
    ).sum()

    # In[6]: Enviar informações para DB (df_ecco_estoque_final)

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_estoque_tamanho_final = df_ecco_estoque_tamanho_final.to_dict(
        orient="records"
    )

    try:
        response = (
            supabase.table(f"mage_bi_estoque_tamanho_{client}_v1")
            .upsert(dic_ecco_estoque_tamanho_final)
            .execute()
        )

    except Exception as exception:
        print(exception)

    # In[7]: Enviar informações para DB (df_ecco_produto)

    dic_ecco_produto = df_ecco_prod.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"mage_bi_produto_{client}_v1")
            .upsert(dic_ecco_produto)
            .execute()
        )

    except Exception as exception:
        print(exception)
