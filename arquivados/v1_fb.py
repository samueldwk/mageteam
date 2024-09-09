# %% FB META ADS API V1

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


def download_fb(cliente, dataname):
    # LONG LIVED ACCESS TOKEN

    llt = os.getenv("fb_llt")

    url_fb_insights = f"https://graph.facebook.com/v18.0/{os.getenv(f'fb_act_{cliente}')}/insights?fields=cpm%2Ccost_per_inline_link_click%2Cspend%2Cwebsite_purchase_roas%2Cinline_link_click_ctr%2Caction_values%2Ccost_per_action_type&time_range[since]={dataname}&time_range[until]={dataname}&level=account&access_token={llt}"

    response = requests.get(url_fb_insights)

    dic_cru_insights = response.json()["data"]
    # print(dic_cru_insights)

    # ===== FLATTEN DICTIONARY ===== #

    flattened_dict = flatten(dic_cru_insights[0])
    # print(flattened_dict)

    # ===== DICTIONARY TO DATA FRAME ===== #

    df_flattened_insights = pd.DataFrame([flattened_dict])

    # ===== IF VALUE IS NOT IN DF, INSERT IT WITH VALUE 0 ===== #

    if "action_values_1_value" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="action_values_1_value",
            value=0,
        )

    if "website_purchase_roas_0_value" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="website_purchase_roas_0_value",
            value=0,
        )

    if "spend" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="spend",
            value=0,
        )

    if "cpm" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="cpm",
            value=0,
        )

    if "cost_per_inline_link_click" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="cost_per_inline_link_click",
            value=0,
        )

    if "inline_link_click_ctr" not in df_flattened_insights.columns:
        df_flattened_insights.insert(
            loc=df_flattened_insights.shape[1],
            column="inline_link_click_ctr",
            value=0,
        )

    # ===== DATAFRAME SELECT COLUMNS ===== #

    df_flattened_insights_selected = df_flattened_insights[
        [
            "spend",
            "action_values_1_value",
            "website_purchase_roas_0_value",
            "cpm",
            "cost_per_inline_link_click",
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
        },
        inplace=True,
    )

    # ===== ALL COLUMNS VALUES TO FLOAT ===== #

    df_flattened_insights_selected = df_flattened_insights_selected.astype(
        float
    )

    # ===== CALCULATE RETORNO COLUMN ===== #

    df_flattened_insights_selected["Retorno"] = (
        df_flattened_insights_selected["Investido"]
        * df_flattened_insights_selected["ROAS"]
    )

    # ===== TRANSFORM CTR TO PORCENTAGE ===== #

    df_flattened_insights_selected["CTR"] = (
        df_flattened_insights_selected["CTR"] / 100
    )

    columns_to_keep = ["Investido", "Retorno", "ROAS", "CPM", "CPC", "CTR"]

    df_flattened_insights_selected = df_flattened_insights_selected[
        columns_to_keep
    ]

    return df_flattened_insights_selected


# for cliente in c_list:
#     df_rel_ger_fb = download_fb(cliente, dataname)
