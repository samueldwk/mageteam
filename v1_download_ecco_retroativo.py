import os
import date_functions_retroativo as datef
import path_functions as pathf
import html_functions_retroativo as htmlf
from datetime import datetime, timedelta
import datetime

import dotenv

dotenv.load_dotenv()

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# Para puxar de uma data específica

# d1 = datetime.datetime(2024, 7, 8).date()

(
    datatxt,
    dataname1,
    datasql,
    datatxt2,
    dataname2,
    datatxt3,
    dataname3,
) = datef.dates(d1)


c_list = [
    # "alanis",
    "basicler",
    # "dadri",
    # "french",
    # "haut",
    # "infini",
    # "kle",
    # "mun",
    "morina",
    # "nobu",
    # "othergirls",
    # "talgui",
    # "paconcept",
    # "rery",
    # "una",
    # "uniquechic",
]

# c_list = ["basicler"]

# In[1] #### FILE PATHS #### d1

# d_path = pathf.dl_folder()
# f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname1}"

# # files = os.listdir(f_path)

# # DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

# htmlf.download_eccosys(datatxt, dataname1, c_list, d_path, f_path)

# In[2] #### FILE PATHS #### d2

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname2}"

# files = os.listdir(f_path)

# DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

htmlf.download_eccosys(datatxt2, dataname2, c_list, d_path, f_path)

# In[3] #### FILE PATHS #### d3

d_path = pathf.dl_folder()
f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname3}"

# files = os.listdir(f_path)

# DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

htmlf.download_eccosys(datatxt3, dataname3, c_list, d_path, f_path)
