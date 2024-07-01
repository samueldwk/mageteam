import pandas as pd
import os
import OleFileIO_PL

# In[1]: Fix Files

#### Fix Product File ####


def fix_prod(xls_path):
    cols1 = ["Codigo SKU"]
    cols2 = ["Preco", "Preco de custo", "Preço Anterior"]

    files = os.listdir(xls_path)

    xls_files = [
        file for file in files if file.endswith(".xls") and "Produtos" in file
    ]

    # Loop through each XLSX file
    for xls_file in xls_files:
        # Construct the full path to the file
        file_path = os.path.join(xls_path, xls_file)

        with open(file_path, "rb") as file:
            ole = OleFileIO_PL.OleFileIO(file)
            if ole.exists("Workbook"):
                d = ole.openstream("Workbook")
                df = pd.read_excel(d, engine="xlrd")

        for col in cols1:
            df[col] = df[col].astype(str)

        for col in cols2:
            df[col] = df[col].str.replace(".", "")
            df[col] = df[col].str.replace(",", ".").astype(float)

        df.to_excel(file_path + "x", index=False)
        os.remove(file_path)


#### Fix Stock File ####


def fix_esto(xls_path):
    cols1 = ["Código"]
    cols2 = ["Un", "Valor unitário", "Valor total"]

    files = os.listdir(xls_path)

    xls_files = [
        file for file in files if file.endswith(".xls") and "Estoque" in file
    ]

    # Loop through each XLSX file
    for xls_file in xls_files:
        # Construct the full path to the file
        file_path = os.path.join(xls_path, xls_file)

        with open(file_path, "rb") as file:
            ole = OleFileIO_PL.OleFileIO(file)
            if ole.exists("Workbook"):
                d = ole.openstream("Workbook")
                df = pd.read_excel(d, engine="xlrd")

        for col in cols1:
            df[col] = df[col].astype(str)

        for col in cols2:
            df = df.drop([col], axis=1)

        df.to_excel(file_path + "x", index=False)
        os.remove(file_path)
