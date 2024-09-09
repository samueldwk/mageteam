# api_eccosys_estoque_v1 gitactions

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

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

# CLIENT LIST
c_list = [
    # "alanis",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    # "mun",
    # "nobu",
    # "othergirls",
    # "rery",
    "talgui",
    # "paconcept",
    # "una",
    # "uniquechic",
]

# c_list = ["talgui"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

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

    # Calcular PorcentagemDescontoProduto
    df_ecco_estoque_final["PorcentagemDescontoProduto"] = 1 - (
        df_ecco_estoque_final["precoProduto"]
        / df_ecco_estoque_final["precoLancamentoProduto"]
    )

    df_ecco_estoque_final.fillna(0, inplace=True)

    # Arredondar o desconto de produto
    precision = 5
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

    # In[11]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_estoque_final = df_ecco_estoque_final.to_dict(orient="records")

    try:
        response = (
            supabase.table("mage_eccosys_estoque_v1")
            .upsert(dic_ecco_estoque_final)
            .execute()
        )

    except Exception as exception:
        print(f"{client}: {exception}")

    print(f"{client}: api_eccosys_estoque_v1 (OK)")
