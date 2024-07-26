import os
import datetime
import date_functions as datef
import path_functions as pathf
import html_functions as htmlf

# import html_functions as htmlf
import dotenv
from datetime import timedelta

dotenv.load_dotenv()

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 1, 12).date()

# datatxt, dataname = datef.dates(d1)

# #Para puxar de um data range específico

data_inicio = datetime.date(2024, 7, 19)

data_fim = datetime.date(2024, 7, 22)

date_range = (data_fim - data_inicio).days + 1

d1 = data_inicio

for dates in range(date_range):
    datatxt, dataname, datasql, dataname2 = datef.dates(d1)

    c_list = [
        # "ajobrand",
        # "alanis",
        # "dadri",
        # "french",
        # "haverroth",
        # "infini",
        # "kle",
        # "luvic",
        # "mun",
        # "nobu",
        # "othergirls",
        # "talgui",
        # "paconcept",
        # "una",
    ]
    c_list = ["paconcept"]

    #### FILE PATHS ####

    d_path = pathf.dl_folder()
    f_path = rf"{os.getenv('path_mage')}m&p\Relatorios Diarios\{dataname}"

    # files = os.listdir(f_path)

    # In[1]: DOWNLOAD FILES FROM ECCOSYS (PRODUTO, VENDAS E ESTOQUE)

    htmlf.download_eccosys(datatxt, dataname, c_list, d_path, f_path)

    d1 = d1 + timedelta(days=1)
