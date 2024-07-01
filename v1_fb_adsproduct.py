# ADS x PRODUCT STOCK

# %% FB META ADS API

import requests
import pandas as pd
import dotenv
import os
from flatten_json import flatten
import datetime
import date_functions as datef


dotenv.load_dotenv()

# c_list = ["french"]

# d1 = datetime.datetime(2024, 1, 28).date()
# datatxt, dataname, dataslq = datef.dates(d1)

# LONG LIVED ACCESS TOKEN

# llt = os.getenv("fb_llt")

url_fb_insights = "https://graph.facebook.com/v20.0/act_468274978174579/campaigns?fields=id%2Cname%2Ceffective_status%2Cinsights%2Cadsets%7Beffective_status%2Cconfigured_status%2Cname%2Cads%7Beffective_status%2Cstatus%2Cname%2Cinsights%7Baction_values%7D%7D%7D&access_token=EAAE6suWmYREBO7RYyylHD86Cy5vh5W6a6yn4qZCsqmKYVUIJadoRACyBdAeGepnZAeFkIeAmfRb57OdzZAAtJils0N8ZCZBOVKCc7biZAn6nDT11gObacudWBIldiLvSGoL9qopieLCDtfY4WiezVpn6Q9LMUNosx1vtbBbS4baHfTl6DSAC1v1ZC37MuPdJyFTKI9BW5dAqGHn6eMZD"

response = requests.get(url_fb_insights)

dic_cru_insights = response.json()["data"]
print(dic_cru_insights)

# ===== FLATTEN DICTIONARY ===== #

flattened_dict = flatten(dic_cru_insights[0])
print(flattened_dict)

# ===== DICTIONARY TO DATA FRAME ===== #

df_flattened_insights = pd.DataFrame([flattened_dict])
