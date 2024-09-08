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

# d1 = datetime.datetime(2024, 9, 5).date()

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

c_list = [
    "alanis",
    "basicler",
    "dadri",
    "french",
    "haut",
    "infini",
    "kle",
    "mun",
    "muna",
    "morina",
    "nobu",
    "othergirls",
    "talgui",
    "paconcept",
    "rery",
    "una",
    "uniquechic",
]

# c_list = ["muna"]

#### FILE PATHS ####

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}"

# files = os.listdir(f_path)

# In[1]: DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

htmlf.download_eccosys(datatxt, dataname, c_list, d_path, f_path)
