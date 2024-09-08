import pandas as pd
import os
import datetime
import date_functions as datef
import path_functions as pathf
import html_functions as htmlf
import file_functions as filef
import dotenv
import gspread


dotenv.load_dotenv()

hj = datetime.datetime.now()
# d1 = datef.dmenos(hj).date()
# #Para puxar de uma data específica
d1 = datetime.datetime(2023, 12, 21).date()

datatxt, dataname = datef.dates(d1)

# c_list = ["ajobrand", "dadri", "french", "infini", "mun", "othergirls", "una"]
c_list = ["french"]

# DICIONÁRIO DE NOMES

dic_nomes = {'ajobrand': 'Ajo Brand', 'dadri': 'Dadri', 'french': 'French', 'infini': 'Infini', 'mun': 'Mun', 'othergirls': 'Other Girls', 'una': 'Una'}

##### DICTS PARA SLICE DO CODIGO SKU #####
c_list_sku_0 = {"Cliente": ["mun", "infini"], "Index": 0}
c_list_sku_8 = {"Cliente": ["talgui", "dadri", "othergirls"], "Index": 8}
c_list_sku_9 = {"Cliente": ["ajobrand", "french", "una"], "Index": 9}

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}"

files = os.listdir(f_path)

# In[1]: FUNCTIONS

### DOWNLOAD FILES FROM ECCOSYS, FB AND GA ###

# htmlf.download_eccosys(datatxt, dataname, c_list, d_path, f_path)
# htmlf.download_fb(datatxt, dataname, c_list, d_path, f_path)
# htmlf.download_ga(datatxt, dataname, c_list, d_path, f_path)

### TRANSFORM XLS INTO XLSX FILES ###

filef.fix_prod(f_path)
filef.fix_esto(f_path)


# %%
### MODELAR DATAFRAME ###

##### ESTOQUE #####

# SKU PARA COD. MODELO + COR

c_list_sku = [c_list_sku_0, c_list_sku_8, c_list_sku_9]

for cliente in c_list:
    f_estoque_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Estoque_{dataname}.xlsx"
    df_estoque = pd.read_excel(f_estoque_path, dtype={"Código": str})

    for c_sku in c_list_sku:
        if cliente in c_sku["Cliente"]:
            index = c_sku["Index"]
            if index == 0:
                df_estoque["Cod. Modelo + Cor"] = df_estoque["Código"].apply(
                    lambda x: x.split("-")[0]
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

    # Merge the DataFrames on 'Cod. Modelo + Cor'
    estoque_preco_df = pd.merge(
        df_estoque_grouped, df_produtos, on="Cod. Modelo + Cor", how="inner"
    )
    estoque_preco_df["Poder de Venda"] = (
        estoque_preco_df["Quantidade"] * estoque_preco_df["Preco Atual"]
    )
    estoque_preco_df["Custo Total"] = (
        estoque_preco_df["Quantidade"] * estoque_preco_df["Preco de Custo"]
    )
    estoque_preco_df.dtypes
    # STOCK STOCK VALUE, QUANTITIY, MKP

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

    ##### VENDAS #####

    cols1 = ["Preco de Venda Produto Un", "Total do Pedido", 'Quantidade']

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

    # SKU to Cod. Modelo + Cor ON df_vendas

    ####### CHANGE SO IT CHANGES TO EACH CLIENT

    # df_vendas["Cod. Modelo + Cor"] = df_vendas["Codigo SKU"].str.slice(0, 9) ### OLD CODE, REPLACE BY NEW

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

    df_vendas = pd.merge(
        df_vendas, df_produtos2, on="Cod. Modelo + Cor", how="inner"
    )

    # ADD CMV TO df_vendas

    df_vendas["CMV"] = df_vendas["Preco de Custo"] * df_vendas["Quantidade"]

    # CALCULAR VALOR DE VENDA PRODUTO TOTAL TO df_vendas

    df_vendas["Valor de Venda Produto Total"] = (
        df_vendas["Preco de Venda Produto Un"] * df_vendas["Quantidade"]
    )

    # COLUMNS TO KEEP ON df_vendas_prod

    columns_to_keep1 = [
        "Data",
        "Situacao",
        "Cod. Modelo + Cor",
        "Quantidade",
        "Preco de Venda Produto Un",
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

    # ADD 'Preco Anterior' OF PRODUCT TO df_vendas_prod

    # df_vendas_prod = pd.merge(
    #     df_vendas_prod, df_produtos2, on="Cod. Modelo + Cor", how="inner"
    # )

    # CALCULAR VALOR DE VENDA PRODUTO TOTAL

    df_vendas_prod["Valor de Venda Produto Total"] = (
        df_vendas_prod["Preco de Venda Produto Un"] * df_vendas_prod["Quantidade"]
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

    # # Create bins for the 'Desconto' column
    bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
    labels = [
        "V: <= 0%",
        "V: > 0% and <= 25%",
        "V: > 25% and <= 45%",
        "V: > 45% and <= 60%",
        "V: > 60%",
    ]

    # # Add a new column 'Desconto Group' based on the bins
    df_vendas_prod_situacao.loc[:, "Faixa de Desconto"] = pd.cut(
        df_vendas_prod_situacao["Desconto"], bins=bins, labels=labels
    )

    # # Use groupby to sum 'Poder de Venda' for each 'Desconto Group'
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


#%% VENDAS + ESTOQUE (JUNTAR DFS DE VENDA E ESTOQUE)

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
    
    df_vendas_ped_total["Mês"] = pd.to_datetime(df_vendas_ped_total["Data"]).dt.month
    df_vendas_ped_total["Ano"] = pd.to_datetime(df_vendas_ped_total["Data"]).dt.year

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

#%% TRAZER DF FB E GA

    ##### FB META ADS #####

    f_fb_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Fb_{dataname}.csv"
    fb_cru = pd.read_csv(f_fb_path)
    fb_cru.dtypes
    
    #  REMOVER CARACTERERS ESPECIAIS E TRANSFORMAR NO TIPO CERTO
    
    for col in fb_cru.columns[1:]:
        fb_cru[col] = fb_cru[col].str.replace(".", "")
        fb_cru[col] = fb_cru[col].str.replace(",", ".")
        fb_cru[col] = fb_cru[col].str.replace('R$ ', '')
        fb_cru[col] = fb_cru[col].str.replace('%', '').astype(float)

    fb_cru['CTR (link click-through rate)'] = fb_cru['CTR (link click-through rate)']/100

    ##### G.A #####

    f_ga_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}\{cliente}_Ga_{dataname}.csv"
    ga_cru = pd.read_csv(f_ga_path)
    
    # DROP DATE
    
    ga_cru.drop('Data', axis=1, inplace = True)
    
    #  REMOVER CARACTERERS ESPECIAIS E TRANSFORMAR NO TIPO CERTO
    
    for col in ga_cru.columns:
        ga_cru[col] = ga_cru[col].str.replace(".", "")
        ga_cru[col] = ga_cru[col].str.replace(",", ".")
        ga_cru[col] = ga_cru[col].str.replace('%', '').astype(float)
        
    ga_cru['Taxa de rejeição'] = ga_cru['Taxa de rejeição']/100 
    ga_cru["Taxa de conversão de sessão"] = ga_cru["Taxa de conversão de sessão"]/100
     
#%% VENDAS/ESTOQUE + FB + GA

    # Juntar todos os dfs

    rel_ger_final = pd.concat([rel_ger, fb_cru, ga_cru], axis=1)

    ########### REORDER COLUMNS ###########

    desired_order = [
        "Data",
        "Mês",
        "Ano",
        "Valor Vendas Total",
        "MKP Total",
        "Quantidade Pedidos",
        "Ticket Medio",
        "Quantidade Estoque",
        "Poder de Venda Total",
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
        "Taxa de rejeição",
        "Taxa de conversão de sessão",
        "Valor gasto",
        "Valor de conversão de compra",
        "Return on ad spend (ROAS)",
        "CPM",
        "CPC (link)",
        "Cost per website purchase",
        "CTR (link click-through rate)",
    ]

    df_rel_ger_final = rel_ger_final[desired_order]

#%% UPDATE GOOGLE SHEETS

    gc = gspread.oauth()
    
    sh = gc.open(f"{dic_nomes[cliente]} - Relatório Gerencial E-Commerce").worksheet("Diário")
    
    df_list_final = df_rel_ger_final.values.tolist()
    
    print(df_list_final)
    
    sh.append_rows(df_list_final, table_range="A1")

