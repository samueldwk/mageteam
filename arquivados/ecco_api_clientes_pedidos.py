# rel_ger_ecco_api_v1.0
# Relatório de cliente

import requests
import pandas as pd
import datetime
import dotenv
import os
import time


dotenv.load_dotenv()

datanamef = "2025-01-01"
datanamet = datetime.datetime.now().date()

# CLIENT LIST
c_list = ["mun"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[4]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000&$dataConsiderada=data&$opcEcommerce=S"

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
            "codigo": "codigoProduto",
            "preco": "precoProduto",
            "situacao": "statusProduto",
            "precoCusto": "precoCustoProduto",
            "idProdutoMaster": "idProdutoPai",
            "precoDe": "precoLancamentoProduto",
        },
        inplace=True,
    )

    # In[2]: Eccosys API: GET Listar todos os pedidos

    url_ped = f"https://empresa.eccosys.com.br/api/pedidos?$fromDate={datanamef}&$toDate={datanamet}&$offset=0&$count=50000&$dataConsiderada=data"

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

    # KEEP ONLY THE ORDERS WITH STATUS NAME = Em aberto, Atendido e Pronto para picking
    status_name_keep = ["Em aberto", "Atendido", "Pronto para picking"]

    df_ecco_ped = df_ecco_ped[
        df_ecco_ped["Status Name"].isin(status_name_keep)
    ]

    # In[5]: Eccosys API: GET Listar todos os produtos de cada pedido

    df_order_ids = df_ecco_ped["id"]

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
        df_ecco_ped, how="left", left_on="idVenda", right_on="id"
    )

    # In[5]: Calcular MKP de venda por pedido

    # COLUMNS TO KEEP
    columns_to_keep = [
        "idVenda",
        "idProduto",
        "quantidade",
        "valor",
    ]

    df_ecco_ped_prod = df_ecco_ped_prod[columns_to_keep]

    # Trazer custo do produto e preco inicial
    df_ecco_ped_prod = df_ecco_ped_prod.merge(
        df_ecco_produto, how="left", left_on="idProduto", right_on="idProduto"
    )

    columns_to_convert = ["precoCustoProduto"]
    df_ecco_ped_prod[columns_to_convert] = df_ecco_ped_prod[
        columns_to_convert
    ].astype(float)

    df_ecco_ped_prod["valor_total_venda_produto"] = (
        df_ecco_ped_prod["valor"] * df_ecco_ped_prod["quantidade"]
    )
    df_ecco_ped_prod["cmv"] = (
        df_ecco_ped_prod["precoCustoProduto"] * df_ecco_ped_prod["quantidade"]
    )

    columns_to_keep = [
        "idVenda",
        "valor_total_venda_produto",
        "cmv",
    ]

    df_ecco_ped_prod_final = df_ecco_ped_prod[columns_to_keep]

    # In[1]: Eccosys API: GET Listar todos os clientes

    url_client = "https://empresa.eccosys.com.br/api/clientes"

    response_client = requests.request(
        "GET", url_client, headers=headers, data=payload, files=files
    )

    dic_ecco_cliente = response_client.json()
    df_ecco_cliente = pd.DataFrame.from_dict(dic_ecco_cliente)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "nome",
        "cep",
        "cidade",
        "uf",
        "dataNascimento",
        "celular",
        "email",
    ]

    df_ecco_cliente = df_ecco_cliente[columns_to_keep]

    # In[3]: Cálculos de cliente x pedidos

    # Agrupar por pedido totalvalorvendasproduto e cmv
    df_ecco_ped_prod_group = (
        df_ecco_ped_prod.groupby("idVenda")
        .agg(
            valor_total_venda_produto=("valor_total_venda_produto", "sum"),
            cmv=("cmv", "sum"),
        )
        .reset_index()
    )

    # Trazer valor_total_venda_produto e cmv para pedidos
    df_ecco_ped_final = df_ecco_ped.merge(
        df_ecco_ped_prod_group, how="left", left_on="id", right_on="idVenda"
    )

    # Agrupar por cliente e calcular a soma e a contagem das vendas
    df_ecco_ped_cliente = (
        df_ecco_ped_final.groupby("idContato")
        .agg(
            Total_Sales=("totalVenda", "sum"),
            Sales_Count=("totalVenda", "size"),
            valor_total_venda_produto=("valor_total_venda_produto", "sum"),
            cmv=("cmv", "sum"),
        )
        .reset_index()
    )

    # Calcular MKP de venda por cliente

    df_ecco_ped_cliente["mkp_vendas"] = (
        df_ecco_ped_cliente["valor_total_venda_produto"]
        / df_ecco_ped_cliente["cmv"]
    )

    # Trazer inf. do cliente para tabela de venda por cliente
    df_ecco_ped_cliente_completo = pd.merge(
        df_ecco_ped_cliente,
        df_ecco_cliente,
        left_on="idContato",
        right_on="id",
        how="left",
    )

    # Substituir strings 'None' por valores NaN para facilitar a conversão
    df_ecco_ped_cliente_completo[
        "dataNascimento"
    ] = df_ecco_ped_cliente_completo["dataNascimento"].replace("None", pd.NA)

    # Converter a coluna aniversário para o formato de data
    df_ecco_ped_cliente_completo["dataNascimento"] = pd.to_datetime(
        df_ecco_ped_cliente_completo["dataNascimento"], errors="coerce"
    )

    def calculate_age(birth_date):
        if pd.isna(birth_date):
            return None
        age = datanamet.year - birth_date.year
        # Ajustar se a data de aniversário ainda não ocorreu este ano
        if (datanamet.month, datanamet.day) < (
            birth_date.month,
            birth_date.day,
        ):
            age -= 1
        return age

    # Aplicar a função para calcular a idade
    df_ecco_ped_cliente_completo["Idade"] = df_ecco_ped_cliente_completo[
        "dataNascimento"
    ].apply(calculate_age)

    # Agrupar venda por idade
    df_ecco_ped_idade = (
        df_ecco_ped_cliente_completo.groupby("Idade")
        .agg(
            Total_Sales=("Total_Sales", "sum"),
            Sales_Count=("Sales_Count", "sum"),
        )
        .reset_index()
    )

    # Cálcular Ticket Médio por Idade

    df_ecco_ped_idade["Ticket Médio"] = (
        df_ecco_ped_idade["Total_Sales"] / df_ecco_ped_idade["Sales_Count"]
    )

    # Agrupar venda por uf
    df_ecco_ped_uf = (
        df_ecco_ped_cliente_completo.groupby("uf")
        .agg(
            Total_Sales=("Total_Sales", "sum"),
            Sales_Count=("Sales_Count", "sum"),
        )
        .reset_index()
    )

    # Cálcular Ticket Médio por uf

    df_ecco_ped_uf["Ticket Médio"] = (
        df_ecco_ped_uf["Total_Sales"] / df_ecco_ped_uf["Sales_Count"]
    )

    # In[6]: Exportar planilhas para Excel

    # Set the directory where you want to save the file
    directory = r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"

    # Construct the full file path
    file_path = os.path.join(
        directory, f"cliente_{client}_({datanamef})-({datanamet}).xlsx"
    )

    # Export df ped_cliente_completo
    df_ecco_ped_cliente_completo.to_excel(file_path, index=False)

    print(
        f"Export Complete: cliente_{client}_({datanamef})-({datanamet}).xlsx"
    )
