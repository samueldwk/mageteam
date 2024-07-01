# %% FB META ADS API

import requests
import pandas as pd
import dotenv
import os
from flatten_json import flatten
import datetime
import date_functions as datef


dotenv.load_dotenv()

# c_list = ["ajobrand"]

# d1 = datetime.datetime(2024, 1, 19).date()
# datatxt, dataname = datef.dates(d1)


def download_fb(cliente, dataname):
    # LONG LIVED ACCESS TOKEN

    llt = os.getenv("fb_llt")

    url_fb_insights = f"https://graph.facebook.com/v18.0/{os.getenv(f'fb_act_{cliente}')}/insights?fields=cpm%2Ccost_per_inline_link_click%2Cspend%2Cwebsite_purchase_roas%2Cinline_link_click_ctr%2Caction_values%2Ccost_per_action_type&time_range[since]={dataname}&time_range[until]={dataname}&level=account&access_token={llt}"

    response = requests.get(url_fb_insights)

    dic_cru_insights = response.json()["data"]
    print(dic_cru_insights)

    # ===== FLATTEN DICTIONARY ===== #

    flattened_dict = flatten(dic_cru_insights[0])
    print(flattened_dict)

    # ===== DICTIONARY TO DATA FRAME ===== #

    df_flattened_insights = pd.DataFrame([flattened_dict])

    # ===== IF action_values_16_value', 'website_purchase_roas_0_value IS NOT IN DF, INSERT IT WITH VALUE 0 ===== #

    if "action_values_16_value" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="action_values_16_value",
            value=0,
        )

    if "website_purchase_roas_0_value" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="website_purchase_roas_0_value",
            value=0,
        )

    # ===== DATAFRAME SELECT COLUMNS ===== #

    df_flattened_insights_selected = df_flattened_insights[
        [
            "spend",
            "action_values_16_value",
            "website_purchase_roas_0_value",
            "cpm",
            "cost_per_inline_link_click",
            "cost_per_action_type_2_value",
            "inline_link_click_ctr",
        ]
    ].copy()

    # ===== DATAFRAME RENAME COLUMNS ===== #

    df_flattened_insights_selected.rename(
        columns={
            "spend": "Investido",
            "cpm": "CPM",
            "cost_per_inline_link_click": "CPC",
            "website_purchase_roas_0_value": "ROAS",
            "inline_link_click_ctr": "CTR",
            "cost_per_action_type_2_value": "CPA",
            "action_values_16_value": "Retorno",
        },
        inplace=True,
    )

    # ===== ALL COLUMNS VALUES TO STR ===== #

    df_flattened_insights_selected = df_flattened_insights_selected.astype(
        float
    )

    # ===== TRANSFORM CTR TO PORCENTAGE ===== #

    df_flattened_insights_selected["CTR"] = (
        df_flattened_insights_selected["CTR"] / 100
    )

    return df_flattened_insights_selected


# for cliente in c_list:
#     df_rel_ger_fb = download_fb(cliente, dataname)
