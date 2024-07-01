import os
import datetime
import date_functions as datef
import path_functions as pathf
import html_functions as htmlf

import dotenv

dotenv.load_dotenv()

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 28).date()

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
    "paconcept",
    "una",
]

# c_list = ["kle"]

#### FILE PATHS ####

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}"

# files = os.listdir(f_path)

# In[1]: DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

htmlf.download_eccosys(datatxt, dataname, c_list, d_path, f_path)
