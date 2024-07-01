### DADRI

# id": "748875959463320

import requests
import pandas as pd
import dotenv
import os
from flatten_json import flatten

dotenv.load_dotenv()

### SHORT LIVED ACCESS TOKEN IMPUT TO GENERATE LONG LIVED TOKEN

# slt = 'EAAE6suWmYREBO0UYYusWonoQFZBO24QRTygGU5o6hZCRMSda2trrPuAw0bdF2VRBNYzDqlFxpiGiVvaHJgZBvzSQBeKjb9P0yTsFFdPauZA9N4DMHkrGskUAK8FoVQqVE2EhtZAbJJA2sE2wTS0WJZAHXUCgm328ZAyqHzmom7e6zdbKHoaZAHpxJuW5ZChsvQDbv0jZCOM2lDefWtdjqZBWIhzlsuY&access_token=EAAE6suWmYREBO0UYYusWonoQFZBO24QRTygGU5o6hZCRMSda2trrPuAw0bdF2VRBNYzDqlFxpiGiVvaHJgZBvzSQBeKjb9P0yTsFFdPauZA9N4DMHkrGskUAK8FoVQqVE2EhtZAbJJA2sE2wTS0WJZAHXUCgm328ZAyqHzmom7e6zdbKHoaZAHpxJuW5ZChsvQDbv0jZCOM2lDefWtdjqZBWIhzlsuY'

# url_llt =  "https://graph.facebook.com/v18.0/oauth/access_token?grant_type=fb_exchange_token&client_id=346015008186641&client_secret=a5a44e818cf7ff57e2523e1b178368b7&fb_exchange_token={slt}"

# response = requests.get(url_llt)

### LONG LIVED ACCESS TOKEN GENERATE

# llt = response.json()['access_token']




#=========== CLIENT LIST =========#

c_list = ["ajobrand", "dadri", "infini", "mun", "othergirls", 'talgui', "una"]
# c_list = ["french"]

#=========== FB API =========#

for cliente in c_list:

    # LONG LIVED ACCESS TOKEN
    
    llt = os.getenv('fb_llt')
    
    url_fb_insights = f"https://graph.facebook.com/v18.0/{os.getenv(f'fb_act_{cliente}')}/insights?fields=cpm%2Ccost_per_inline_link_click%2Cspend%2Cwebsite_purchase_roas%2Cinline_link_click_ctr%2Caction_values%2Ccost_per_action_type&date_preset=yesterday&level=account&access_token={llt}"
    
    response = requests.get(url_fb_insights)
    
    dic_cru_insights = response.json()['data']
    print(dic_cru_insights)
    
    # ===== FLATTEN DICTIONARY ===== #
    
    flattened_dict = flatten(dic_cru_insights[0])
    print(flattened_dict)
    
    # ===== DICTIONARY TO DATA FRAME ===== #
    
    df_flattened_insights = pd.DataFrame([flattened_dict])
    
    # ===== DATAFRAME SELECT COLUMNS ===== #
    
    df_flattened_insights_selected = df_flattened_insights[['spend', 'action_values_16_value','website_purchase_roas_0_value', 'cpm', 'cost_per_inline_link_click', 'cost_per_action_type_2_value', 'inline_link_click_ctr']].copy()
    
    # ===== DATAFRAME RENAME COLUMNS ===== #
    
    df_flattened_insights_selected.rename(columns = {'spend':'Investido', 'cpm': 'CPM', 'cost_per_inline_link_click': 'CPC', 'website_purchase_roas_0_value': 'ROAS', 'inline_link_click_ctr': 'CTR', 'cost_per_action_type_2_value': 'CPA', 'action_values_16_value': 'Retorno'}, inplace = True)