import pandas as pd
import os
import datetime
import date_functions as datef
import path_functions as pathf
import file_functions as filef
import dotenv
import gspread
import v1_fb as fb


dotenv.load_dotenv()

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 7, 12).date()

datatxt, dataname, datasql, dataname2 = datef.dates(d1)

c_list = [
    # "ajobrand",
    "alanis",
    "dadri",
    "french",
    "haverroth",
    "haut",
    "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "talgui",
    "una",
]

# c_list = ["talgui"]

# DICIONÁRIO DE NOMES

dic_nomes = {
    "ajobrand": "aJo Brand",
    "alanis": "Alanis",
    "dadri": "Dadri",
    "french": "French",
    "haverroth": "Haverroth",
    "haut": "Haut",
    "infini": "Infini",
    "kle": "Kle",
    "luvic": "Luvic",
    "mun": "Mun",
    "nobu": "Nobu",
    "othergirls": "Other Girls",
    "paconcept": "P.A Concept",
    "talgui": "Talgui",
    "una": "Una",
}

##### DICTS PARA SLICE DO CODIGO SKU #####

c_list_sku_0 = {"Cliente": ["infini", "luvic", "mun"], "Index": 0}
c_list_sku_7 = {"Cliente": ["nobu"], "Index": 7}
c_list_sku_8 = {
    "Cliente": ["alanis", "dadri", "othergirls", "talgui"],
    "Index": 8,
}
c_list_sku_9 = {"Cliente": ["ajobrand", "french", "una"], "Index": 9}
c_list_sku_15 = {
    "Cliente": ["haverroth", "haut", "kle", "paconcept"],
    "Index": 15,
}  ###get all sku, doesnt need to slice

#### FILE PATHS ####

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}"

# In[1]: TRANSFORM XLS INTO XLSX FILES ###

filef.fix_prod(f_path)
filef.fix_esto(f_path)

# %% MODELAR DATAFRAME DE ESTOQUE E VENDAS

############################### ESTOQUE ##################################

# SKU PARA COD. MODELO + COR

c_list_sku = [
    c_list_sku_0,
    c_list_sku_7,
    c_list_sku_8,
    c_list_sku_9,
    c_list_sku_15,
]

for cliente in c_list:
    f_estoque_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Estoque_{dataname}.xlsx"
    df_estoque = pd.read_excel(f_estoque_path, dtype={"Código": str})

    for c_sku in c_list_sku:
        if cliente in c_sku["Cliente"]:
            index = c_sku["Index"]
            if index == 0:
                df_estoque["Cod. Modelo + Cor"] = (
                    df_estoque["Código"]
                    .astype(str)
                    .apply(lambda x: x.split("-")[0])
                )
            else:
                df_estoque["Cod. Modelo + Cor"] = df_estoque[
                    "Código"
                ].str.slice(0, index)
            break
    else:
        print(f"The value '{cliente}' is not present in any c_list_sku.")

    # GROUP STOCK BY 'COD. MODELO + COR'

    df_estoque_grouped = df_estoque.groupby(["Cod. Modelo + Cor"]).agg(
        {"Quantidade": "sum"}
    )

    # ESTOQUE + PRECOS

    # IMPORT EXCEL PRODUTOS TO FIX AND ORGANIZE
    f_produtos_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Produtos_{dataname}.xlsx"
    df_produtos = pd.read_excel(
        f_produtos_path, dtype={"Codigo SKU": str}
    ).rename(
        columns={
            "Codigo SKU": "Cod. Modelo + Cor",
            "Preco": "Preco Atual",
            "Preço Anterior": "Preco Anterior",
            "Preco de custo": "Preco de Custo",
        }
    )
    df_produtos["Desconto"] = 1 - (
        df_produtos["Preco Atual"] / df_produtos["Preco Anterior"]
    )

    # CLIENTS WITH ONLY PRODUTO FILHO

    if cliente == "kle":
        for c_sku in c_list_sku:
            if cliente in c_sku["Cliente"]:
                index = c_sku["Index"]
                if index == 0:
                    df_produtos["Cod. Modelo + Cor"] = df_produtos[
                        "Cod. Modelo + Cor"
                    ].apply(lambda x: x.split("-")[0])
                else:
                    df_produtos["Cod. Modelo + Cor"] = df_produtos[
                        "Cod. Modelo + Cor"
                    ].str.slice(0, index)
                break
        else:
            print(f"The value '{cliente}' is not present in any c_list_sku.")

    if cliente == "kle":
        df_produtos3 = df_produtos.drop_duplicates(
            subset=["Cod. Modelo + Cor"]
        )

        # Merge the DataFrames on 'Cod. Modelo + Cor'
        estoque_preco_df = pd.merge(
            df_estoque_grouped,
            df_produtos3,
            on="Cod. Modelo + Cor",
            how="left",
        )

    else:
        # Merge the DataFrames on 'Cod. Modelo + Cor'
        estoque_preco_df = pd.merge(
            df_estoque_grouped,
            df_produtos,
            on="Cod. Modelo + Cor",
            how="left",
        )

    estoque_preco_df["Poder de Venda"] = (
        estoque_preco_df["Quantidade"] * estoque_preco_df["Preco Atual"]
    )
    estoque_preco_df["Custo Total"] = (
        estoque_preco_df["Quantidade"] * estoque_preco_df["Preco de Custo"]
    )
    estoque_preco_df.dtypes

    #### STOCK STOCK VALUE, QUANTITIY, MKP

    # EXCLUIR CÓDIGOS QUE NÃO DEVEM SER SOMADOS AO CÁLCULO DE ESTOQUE POR CLIENTE

    if cliente == "una":
        estoque_preco_df = estoque_preco_df[
            ~estoque_preco_df.apply(
                lambda row: row.astype(str).str.contains("VP").any(), axis=1
            )
        ]

    if cliente == "othergirls":
        estoque_preco_df = estoque_preco_df[
            ~estoque_preco_df.apply(
                lambda row: row.astype(str).str.contains("BOHOCARD").any(),
                axis=1,
            )
        ]

    if cliente == "mun":
        estoque_preco_df = estoque_preco_df[
            ~estoque_preco_df.apply(
                lambda row: row.astype(str).str.contains("VP").any(), axis=1
            )
        ]

    # Quantidade total
    estoque_preco_total_df = pd.DataFrame(
        {
            "Quantidade Estoque": [estoque_preco_df["Quantidade"].sum()],
            "Poder de Venda Total": [estoque_preco_df["Poder de Venda"].sum()],
            "Custo do Estoque": [estoque_preco_df["Custo Total"].sum()],
        }
    )

    estoque_preco_total_df["MKP"] = (
        estoque_preco_total_df["Poder de Venda Total"]
        / estoque_preco_total_df["Custo do Estoque"]
    )

    estoque_preco_total_df["Preço Médio"] = (
        estoque_preco_total_df["Poder de Venda Total"]
        / estoque_preco_total_df["Quantidade Estoque"]
    )

    ########## STOCK VALUE PER DISCOUNT RANGE ##########

    # Round the 'Desconto' values to a reasonable precision
    precision = 5  # Adjust the precision as needed
    estoque_preco_df["Desconto"] = estoque_preco_df["Desconto"].round(
        precision
    )

    # Create bins for the 'Desconto' column
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "E: <= 0%",
        "E: > 0% and <= 25%",
        "E: > 25% and <= 45%",
        "E: > 45% and <= 60%",
        "E: > 60%",
    ]

    # Add a new column 'Desconto Group' based on the bins
    estoque_preco_df["Faixa de Desconto"] = pd.cut(
        estoque_preco_df["Desconto"], bins=bins, labels=labels
    )

    # Use groupby to sum 'Poder de Venda' for each 'Desconto Group'
    result = (
        estoque_preco_df.groupby("Faixa de Desconto", observed=False)[
            "Poder de Venda"
        ]
        .sum()
        .reset_index()
    )

    # Create a new DataFrame 'estoque_desconto' with the results
    estoque_desconto = pd.DataFrame(result)

    # CHANGE COLUMNS FOR LINES

    estoque_desconto_final = estoque_desconto.T

    ############################ VENDAS ##############################

    cols1 = [
        "Preco de Venda Produto Un",
        "Total do Pedido",
        "Quantidade",
        "Desconto item",
    ]

    # LIMPAR E ORGANIZAR DADOS

    f_vendas_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Vendas_{dataname}.csv"
    df_vendas = pd.read_csv(
        f_vendas_path,
        sep=";",
        dtype={
            "Código SKU": str,
            "Preco de Venda": float,
            "Total do Pedido": float,
            "Quantidade": str,
        },
    ).rename(
        columns={
            "Preço de Venda": "Preco de Venda Produto Un",
            "Situação": "Situacao",
            "Código SKU": "Codigo SKU",
            "Número do Pedido": "Numero do Pedido",
            "Total do pedido": "Total do Pedido",
            "Preço de Custo": "Ignorar",
        }
    )

    df_vendas.dtypes

    for col in cols1:
        df_vendas[col] = df_vendas[col].str.replace(".", "")
        df_vendas[col] = df_vendas[col].str.replace(",", ".").astype(float)

    # TEMPORARY FIX TO 'nobu'SKU, HAVING DIFFICULT SINCE SKU STARTS WITH 0

    if cliente == "nobu":
        df_vendas["Codigo SKU"] = df_vendas["Codigo SKU"].astype(int)

    if cliente == "nobu":
        df_vendas["Codigo SKU"] = df_vendas["Codigo SKU"].astype(str)

    if cliente == "kle":
        df_vendas["Codigo SKU"] = df_vendas["Codigo SKU"].astype(str)

    # SKU to Cod. Modelo + Cor ON df_vendas

    ####### CHANGE SO IT CHANGES TO EACH CLIENT

    for c_sku in c_list_sku:
        if cliente in c_sku["Cliente"]:
            index = c_sku["Index"]
            if index == 0:
                df_vendas["Cod. Modelo + Cor"] = df_vendas["Codigo SKU"].apply(
                    lambda x: x.split("-")[0]
                )
            else:
                df_vendas["Cod. Modelo + Cor"] = df_vendas[
                    "Codigo SKU"
                ].str.slice(0, index)
            break
    else:
        print(f"The value '{cliente}' is not present in any c_list_sku.")

    # ADD COST OF PRODUCT, COD. MODELO + COR, PRECO ANTERIOR , DESCONTO TO df_vendas

    columns_to_keep1 = [
        "Preco de Custo",
        "Cod. Modelo + Cor",
        "Preco Anterior",
    ]
    df_produtos2 = df_produtos[
        columns_to_keep1
    ].copy()  # Make a copy to avoid modifying the original DataFrame

    if cliente == "kle":
        df_produtos2 = df_produtos2.drop_duplicates(
            subset=["Cod. Modelo + Cor"]
        )

    df_vendas = pd.merge(
        df_vendas, df_produtos2, on="Cod. Modelo + Cor", how="left"
    )

    # ADD CMV TO df_vendas

    df_vendas["CMV"] = df_vendas["Preco de Custo"] * df_vendas["Quantidade"]

    # CALCULAR VALOR DE VENDA PRODUTO TOTAL TO df_vendas

    df_vendas["Valor de Venda Produto Total"] = (
        df_vendas["Preco de Venda Produto Un"] * df_vendas["Quantidade"]
    ) - df_vendas["Desconto item"]

    # COLUMNS TO KEEP ON df_vendas_prod

    columns_to_keep1 = [
        "Data",
        "Situacao",
        "Cod. Modelo + Cor",
        "Quantidade",
        "Preco de Venda Produto Un",
        "Valor de Venda Produto Total",
        "Preco Anterior",
    ]
    df_vendas_prod = df_vendas[
        columns_to_keep1
    ].copy()  # Make a copy to avoid modifying the original DataFrame

    # ADD COLUMN DISCONT ON df_vendas_prod

    df_vendas_prod["Desconto"] = 1 - (
        df_vendas_prod["Preco de Venda Produto Un"]
        / df_vendas_prod["Preco Anterior"]
    )

    # CALCULATE SALES VALUE OF ORDERS WITH ACCEPTABLE SITUATION: ATENDIDO; EM ABERTO; PRONTO PARA PICKING

    columns_to_keep2 = [
        "Data",
        "Numero do Pedido",
        "Situacao",
        "Total do Pedido",
        "CMV",
        "Valor de Venda Produto Total",
    ]
    df_vendas_ped = df_vendas[
        columns_to_keep2
    ].copy()  # Make a copy to avoid modifying the original DataFrame

    # Filter rows based on conditions in 'Situacao'

    condition = df_vendas_ped["Situacao"].isin(
        ["Em aberto", "Atendido", "Pronto para picking"]
    )
    filtered_df = df_vendas_ped[condition]

    # Sum 'Total do Pedido' based on the filtered conditions

    df_vendas_ped_total = (
        filtered_df.groupby("Data")["Total do Pedido"].sum().reset_index()
    )

    # Rename the 'Total do Pedido' column to 'Valor Vendas Total'

    df_vendas_ped_total = df_vendas_ped_total.rename(
        columns={"Total do Pedido": "Valor Vendas Total"}
    )

    # ADD CMV TO df_vendas_ped_total

    df_vendas_ped_total["CMV Total"] = (
        filtered_df.groupby("Data")["CMV"].sum().reset_index()["CMV"]
    )

    # ADD VALOR DE VENDA PRODUTO TOTAL TO df_vendas_ped_total

    df_vendas_ped_total["Valor de Venda Produto Total"] = (
        filtered_df.groupby("Data")["Valor de Venda Produto Total"]
        .sum()
        .reset_index()["Valor de Venda Produto Total"]
    )

    # ADD MKP TOTAL TO df_vendas_ped_total

    df_vendas_ped_total["MKP Total"] = (
        df_vendas_ped_total["Valor de Venda Produto Total"]
        / df_vendas_ped_total["CMV Total"]
    )

    # ADD QUANTITY OF ORDERS TO df_vendas_ped_total

    df_vendas_ped_total["Quantidade Pedidos"] = filtered_df[
        "Numero do Pedido"
    ].nunique()

    # ADD TICKET MEDIO TO df_vendas_ped_total

    df_vendas_ped_total["Ticket Medio"] = (
        df_vendas_ped_total["Valor Vendas Total"]
        / df_vendas_ped_total["Quantidade Pedidos"]
    )

    ########## VENDAS POR DESCONTO ##########

    ### CALCULATE SALES VALUE OF ORDERS WITH ACCEPTABLE SITUATION: ATENDIDO; EM ABERTO; PRONTO PARA PICKING ###

    columns_to_keep2 = [
        "Data",
        "Desconto",
        "Situacao",
        "Valor de Venda Produto Total",
    ]
    df_vendas_prod = df_vendas_prod[
        columns_to_keep2
    ].copy()  # Make a copy to avoid modifying the original DataFrame

    # # Filter rows based on conditions in 'Situacao'

    condition = df_vendas_prod["Situacao"].isin(
        ["Em aberto", "Atendido", "Pronto para picking"]
    )
    filtered_df2 = df_vendas_prod[condition]

    # # Sum 'Total do Pedido' based on the filtered conditions

    df_vendas_prod_situacao = filtered_df2.copy()

    # ### SEPARATE SALES PER DISCOUNT ###

    # # Round the 'Desconto' values to a reasonable precision
    precision = 5  # Adjust the precision as needed
    df_vendas_prod_situacao.loc[:, "Desconto"] = df_vendas_prod_situacao[
        "Desconto"
    ].round(precision)

    # Create bins for the 'Desconto' column
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
    ]

    # Add a new column 'Desconto Group' based on the bins
    df_vendas_prod_situacao.loc[:, "Faixa de Desconto"] = pd.cut(
        df_vendas_prod_situacao["Desconto"], bins=bins, labels=labels
    )

    # SUBSTITUTE ANY NAN BY 0 IN COLUMN DESCONTO POR FAIXA DE PREÇO

    df_vendas_prod_situacao["Faixa de Desconto"] = df_vendas_prod_situacao[
        "Faixa de Desconto"
    ].fillna("V: <= 0%")

    # Use groupby to sum 'Poder de Venda' for each 'Desconto Group'
    result1 = (
        df_vendas_prod_situacao.groupby("Faixa de Desconto", observed=False)[
            "Valor de Venda Produto Total"
        ]
        .sum()
        .reset_index()
    )

    # # Create a new DataFrame 'estoque_desconto' with the results
    venda_desconto = pd.DataFrame(result1)
    venda_desconto_final = venda_desconto.T

    # %% VENDAS + ESTOQUE (JUNTAR DFS DE VENDA E ESTOQUE)

    # Colocar segunda coluna como nome coluna

    # Assuming 'estoque_desconto_final' and 'venda_desconto_final' are your DataFrames
    estoque_desconto_final.columns = estoque_desconto_final.iloc[0]
    venda_desconto_final.columns = venda_desconto_final.iloc[0]

    # Drop the first row (index 0) after setting the column names
    estoque_desconto_final = estoque_desconto_final.iloc[1:].reset_index(
        drop=True
    )
    venda_desconto_final = venda_desconto_final.iloc[1:].reset_index(drop=True)

    # ACRESCENTAR MÊS E ANO NAS COLUNAS

    df_vendas_ped_total["Mês"] = pd.to_datetime(
        df_vendas_ped_total["Data"]
    ).dt.month
    df_vendas_ped_total["Ano"] = pd.to_datetime(
        df_vendas_ped_total["Data"]
    ).dt.year

    # Juntar todos os dfs

    rel_ger = pd.concat(
        [
            df_vendas_ped_total,
            estoque_preco_total_df,
            venda_desconto_final,
            estoque_desconto_final,
        ],
        axis=1,
    )

    # %% REORDER COLUMNS IN FINAL DF

    ########### REORDER COLUMNS ###########

    desired_order = [
        "Data",
        "Mês",
        "Ano",
        "Valor Vendas Total",
        "MKP Total",
        "Quantidade Pedidos",
        "CMV Total",
        "Ticket Medio",
        "Quantidade Estoque",
        "Poder de Venda Total",
        "Preço Médio",
        "MKP",
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
        "E: <= 0%",
        "E: > 0% and <= 25%",
        "E: > 25% and <= 45%",
        "E: > 45% and <= 60%",
        "E: > 60%",
    ]

    df_rel_ger_ecco = rel_ger[desired_order]

    # %% FB ADS DATA

    df_rel_ger_fb = fb.download_fb(cliente, dataname)

    # %% CONCAT 'df_rel_ger_ecco', 'df_rel_ger_fb'

    df_rel_ger = pd.concat([df_rel_ger_ecco, df_rel_ger_fb], axis=1)

    # %% IF SALES = 0, SUBSTITUTE NAN FOR 0 AND PUT DATE

    df_rel_ger["Data"] = datatxt
    df_rel_ger["Mês"] = pd.to_datetime(
        df_rel_ger["Data"], dayfirst=True
    ).dt.month
    df_rel_ger["Ano"] = pd.to_datetime(
        df_rel_ger["Data"], dayfirst=True
    ).dt.year

    df_rel_ger.fillna(0, inplace=True)

    # %% UPDATE GOOGLE SHEETS

    gc = gspread.oauth()

    sh = gc.open(
        f"{dic_nomes[cliente]} - Relatório Gerencial E-Commerce"
    ).worksheet("Diário")

    df_list_final = df_rel_ger.values.tolist()

    print(df_list_final)

    sh.append_rows(df_list_final, table_range="A1")
