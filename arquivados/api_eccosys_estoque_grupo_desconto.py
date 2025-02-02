# estoque por grupo x faixa de preço (api_eccosys_estoque_grupo_desconto)

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
    "mun",
    # "muna",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
    "uniquechic",
]

c_list = ["basicler"]

# API HEADER

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
        df_ecco_prod[columns_to_convert] = df_ecco_prod[
            columns_to_convert
        ].astype(int)

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

        # Colocar coluna grupo de produtos
        df_ecco_estoque_final["grupo_produto"] = (
            df_ecco_estoque_final["nomeProduto"].str.split(" ").str[0]
        )

        # Colocar coluna de valor_venda_estoque
        df_ecco_estoque_final["valor_venda_estoque"] = (
            df_ecco_estoque_final["estoqueDisponivel"]
            * df_ecco_estoque_final["precoProduto"]
        )

        # Filtrar apenas produtos ativos
        df_ecco_estoque_ativo_final = df_ecco_estoque_final[
            df_ecco_estoque_final["statusProduto"] == "A"
        ]

        # COLUMNS TO KEEP para dataframe final que vai ir para o banco
        columns_to_keep = [
            # "estoqueReal",
            # "estoqueDisponivel",
            # "codigoProduto",
            # "nomeProduto",
            # "idProduto",
            # "precoLancamentoProduto",
            # "precoProduto",
            "valor_venda_estoque",
            # "precoCustoProdutoUnit",
            # "statusProduto",
            # "PorcentagemDescontoProduto",
            # "FaixaDescontoProduto",
            "grupo_produto",
        ]

        df_ecco_estoque_final = df_ecco_estoque_final[columns_to_keep]

        # Juntar grupos regata, colete, top e cropped com blusa
        df_ecco_estoque_final["grupo_produto"] = df_ecco_estoque_final[
            "grupo_produto"
        ].replace(["Regata", "Colete", "Top", "Cropped"], "Blusa")

        # Agrupar estoque por grupo
        df_ecco_estoque_agrupado_final = df_ecco_estoque_final.groupby(
            "grupo_produto"
        ).sum()

        # Colocar coluna mage_cliente
        df_ecco_estoque_agrupado_final["mage_cliente"] = client

        # Colocar coluna data estoque
        df_ecco_estoque_agrupado_final["data_estoque"] = dataname

    except Exception as e:
        print(
            f"*****ERRO: {client} | api_eccosys_estoque_grupo_desconto | {e} [{dataname}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | api_eccosys_estoque_grupo_desconto | {e} [{dataname}]"
        )
