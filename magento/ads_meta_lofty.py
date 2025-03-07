# %% FB META ADS API V2

import requests
import pandas as pd
import dotenv
import os
import datetime
import date_functions as datef
import numpy as np
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import time


dotenv.load_dotenv()

c_list = ["lofty"]

hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# Para puxar de uma data específica
# d1 = datetime.datetime(2025, 3, 3).date()

datatxt, dataname, datasql, dataname2, dataname3, dataname4 = datef.dates(d1)

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_LOFTY_URL")
key: str = os.environ.get("SUPABASE_LOFTY_KEY")
supabase: Client = create_client(url, key)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2),
    retry=retry_if_exception_type(Exception),
)
def download_fb(client, dataname):
    global df_fb_campaigns_cru_global  # Declare it as global

    # LONG LIVED ACCESS TOKEN
    llt = os.getenv("fb_llt")
    act_id = os.getenv(f"fb_act_{client}")

    if not act_id:
        print(f"❌ Error: Missing Facebook account ID for client {client}")
        return pd.DataFrame()  # Return an empty DataFrame

    url_fb_insights = f"https://graph.facebook.com/v19.0/{os.getenv(f'fb_act_{client}')}?fields=campaigns%7Binsights.time_range(%7B'since'%3A'{dataname}'%2C'until'%3A'{dataname}'%7D)%7Bobjective%2Cspend%2Caction_values%2Cimpressions%2Cinline_link_clicks%2Ccampaign_name%7D%7D&access_token={llt}"

    def fetch_all_pages(url):
        """Fetch all pages of data from Facebook API."""
        all_results = []
        nextnumber = 0

        while url:
            print(f"📡 Fetching data from: {url}")  # Debugging

            response = requests.get(url)

            if response.status_code != 200:
                print(f"❌ API Error: {response.status_code}")
                print(f"❌ Response content: {response.text}")
                break

            data = response.json()

            # Debug: Print the response structure
            print(f"📝 API Response: {data}")

            # Extract campaign data
            campaigns = (
                data.get("campaigns", {}).get("data", [])
                if nextnumber == 0
                else data.get("data", [])
            )
            all_results.extend(campaigns)

            # Get next page URL
            url = (
                data.get("campaigns", {}).get("paging", {}).get("next")
                if nextnumber == 0
                else data.get("paging", {}).get("next")
            )

            nextnumber += 1

        return all_results

    # Fetch all pages
    campaigns = fetch_all_pages(url_fb_insights)

    if not campaigns:
        print("⚠️ No campaign data retrieved.")
        return pd.DataFrame()  # Return empty DataFrame

    # Initialize structured data list
    structured_data = []

    # Extract campaign details
    for campaign in campaigns:
        campaign_id = campaign.get("id", "Unknown")
        insights = campaign.get("insights", {}).get("data", [])

        for insight in insights:
            row = {
                "Campaign ID": campaign_id,
                "Campaign Name": insight.get("campaign_name", "Unknown"),
                "Objective": insight.get("objective", "Unknown"),
                "Spend": float(insight.get("spend", 0)),
                "Impressions": float(insight.get("impressions", 0)),
                "Link Clicks": float(insight.get("inline_link_clicks", 0)),
                "Date Start": insight.get("date_start", "N/A"),
                "Date Stop": insight.get("date_stop", "N/A"),
            }

            # Add action values
            action_values = insight.get("action_values", [])
            if isinstance(action_values, list):
                for action in action_values:
                    action_type = action.get("action_type", "Unknown")
                    value = float(action.get("value", 0))
                    row[action_type] = value

            structured_data.append(row)

    # Create DataFrame
    df_fb_campaigns_cru = pd.DataFrame(structured_data)

    df_fb_campaigns_cru_global = df_fb_campaigns_cru  # Store it globally

    if df_fb_campaigns_cru.empty:
        print("⚠️ No data available after processing.")
        return df_fb_campaigns_cru  # Return empty DataFrame safely

    # Replace None values with 0
    df_fb_campaigns_cru.fillna(0, inplace=True)

    # Ensure 'web_in_store_purchase' column exists
    if "web_in_store_purchase" not in df_fb_campaigns_cru.columns:
        df_fb_campaigns_cru["web_in_store_purchase"] = 0

    # In[02]: Calculate final Facebook metrics

    # Convert data types
    columns_to_convert = [
        "Spend",
        "Impressions",
        "Link Clicks",
        "web_in_store_purchase",
    ]
    df_fb_campaigns_cru[columns_to_convert] = df_fb_campaigns_cru[
        columns_to_convert
    ].astype(float)

    # # Filter conversion campaigns
    # df_fb_campaigns_filtrado = df_fb_campaigns_cru[
    #     df_fb_campaigns_cru["Objective"].isin(
    #         ["OUTCOME_SALES", "CONVERSIONS", "PRODUCT_CATALOG_SALES"]
    #     )
    # ]

    # # Remove wholesale campaigns
    # df_fb_campaigns_filtrado = df_fb_campaigns_filtrado[
    #     ~df_fb_campaigns_filtrado.apply(
    #         lambda row: row.astype(str).str.contains("atacado").any(), axis=1
    #     )
    # ]

    # Sum total spend
    resultado_fb_spend_total = df_fb_campaigns_cru["Spend"].sum()

    # Sum impressions
    resultado_fb_impressions_total = df_fb_campaigns_cru["Impressions"].sum()

    # Sum link clicks
    resultado_fb_linkclicks_total = df_fb_campaigns_cru["Link Clicks"].sum()

    # Sum total sales
    resultado_fb_vendas_total = df_fb_campaigns_cru[
        "web_in_store_purchase"
    ].sum()

    # Calculate CPM (Spend / Impressions * 1000)
    resultado_fb_cpm = (
        (resultado_fb_spend_total / resultado_fb_impressions_total) * 1000
        if resultado_fb_impressions_total > 0
        else 0
    )

    # Calculate CPC (Spend / Clicks)
    resultado_fb_cpc = (
        resultado_fb_spend_total / resultado_fb_linkclicks_total
        if resultado_fb_linkclicks_total > 0
        else 0
    )

    # Calculate CTR (Clicks / Impressions)
    resultado_fb_ctr = (
        resultado_fb_linkclicks_total / resultado_fb_impressions_total
        if resultado_fb_impressions_total > 0
        else 0
    )

    # Calculate ROAS (Sales / Spend)
    resultado_fb_roas = (
        resultado_fb_vendas_total / resultado_fb_spend_total
        if resultado_fb_spend_total > 0
        else 0
    )

    # Create final summary DataFrame
    df_relger_fb_final = pd.DataFrame(
        {
            "ads_meta_investment": [resultado_fb_spend_total],
            "ads_meta_sale": [resultado_fb_vendas_total],
            "ads_meta_roas": [resultado_fb_roas],
            "ads_meta_cpm": [resultado_fb_cpm],
            "ads_meta_cpc": [resultado_fb_cpc],
            "ads_meta_ctr": [resultado_fb_ctr],
        }
    )

    # Replace NaN and Inf values with 0
    df_relger_fb_final.replace([np.inf, -np.inf], 0, inplace=True)
    df_relger_fb_final.fillna(0, inplace=True)

    # Adicionar a coluna de data do metaads
    df_relger_fb_final["ads_meta_date"] = dataname

    return df_relger_fb_final


# MetaAds API

for client in c_list:
    metaAds = download_fb(client, dataname)
    print(metaAds)

    # Gravar no banco informações do metaads

    dic_metaAds = metaAds.to_dict(orient="records")

    response = (
        supabase.table("MetaAds")
        .upsert(dic_metaAds, returning="minimal")
        .execute()
    )
