# rel_ger_fb_api_v1.0
# REL GER FACEBOOK BY API

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import numpy as np


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# Para puxar de uma data específica

# d1 = datetime.datetime(2024, 7, 5).date()

datatxt1, dataname1, datasql, dataname2 = datef.dates(d1)

date_object = datetime.datetime.strptime(dataname1, "%Y-%m-%d")

month = date_object.month
year = date_object.year

# CLIENT LIST
c_list = [
    # "ajobrand",
    "alanis",
    "dadri",
    "french",
    # "haverroth",
    "haut",
    "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
]

# c_list = ["alanis"]


for client in c_list:
    # In[01]: Meta ADS API

    llt = os.getenv("fb_llt")

    url_fb_insights = f"https://graph.facebook.com/v19.0/{os.getenv(f'fb_act_{client}')}?fields=campaigns%7Binsights.time_range(%7B'since'%3A'{dataname1}'%2C'until'%3A'{dataname1}'%7D)%7Bobjective%2Cspend%2Caction_values%2Cimpressions%2Cinline_link_clicks%2Ccampaign_name%7D%7D&access_token={llt}"

    def fetch_all_pages(url):
        all_results = []
        nextnumber = 0

        while url:
            # print(
            #     f"Fetching data from URL: {url}"
            # )  # Debug: Print the current URL
            response = requests.get(url)

            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                break

            data = response.json()

            # Debug: Print the current page data
            print(f"Response Data: {data}")

            # Append campaign data from the current page

            if nextnumber == 0:
                campaigns = data.get("campaigns", {}).get("data", [])
                all_results.extend(campaigns)

            else:
                campaigns = data.get("data", [])
                all_results.extend(campaigns)

            # Get the URL for the next page, if it exists

            if nextnumber == 0:
                url = data.get("campaigns", {}).get("paging", {}).get("next")

            else:
                url = data.get("paging", {}).get("next")

            # # Debug: Print the next URL
            # print(f"Next URL: {url}")

            nextnumber = nextnumber + 1

        # print(f"Nextnumber: {nextnumber}")
        return all_results

    # Fetch all pages
    campaigns = fetch_all_pages(url_fb_insights)

    structured_data = []

    # Extract and structure the data
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        insights = campaign.get("insights", {}).get("data", [])

        for insight in insights:
            row = {
                "Campaign ID": campaign_id,
                "Campaign Name": insight.get("campaign_name"),
                "Objective": insight.get("objective"),
                "Spend": insight.get("spend"),
                "Impressions": insight.get("impressions"),
                "Link Clicks": insight.get("inline_link_clicks"),
                "Date Start": insight.get("date_start"),
                "Date Stop": insight.get("date_stop"),
            }

            # Add action values
            action_values = insight.get("action_values", [])
            for action in action_values:
                action_type = action.get("action_type")
                value = action.get("value")
                row[action_type] = value

            structured_data.append(row)

    # Create DataFrame
    df_fb_campaigns_cru = pd.DataFrame(structured_data)

    # Replace None values with 0
    df_fb_campaigns_cru = df_fb_campaigns_cru.fillna(0)

    # If no "web_in_store_purchase" on df_fb_campaigns add column if value 0

    if "web_in_store_purchase" not in df_fb_campaigns_cru.columns:
        df_fb_campaigns_cru["web_in_store_purchase"] = 0

    # In[02]: Calcular os valores finais de facebook

    # CONVERT COLUMNS TYPE
    columns_to_convert = [
        "Spend",
        "Impressions",
        "Link Clicks",
        "web_in_store_purchase",
    ]
    df_fb_campaigns_cru[columns_to_convert] = df_fb_campaigns_cru[
        columns_to_convert
    ].astype(float)

    # Filtrar apenas campanhas de objetivo conversão / vendas
    df_fb_campaigns_filtrado = df_fb_campaigns_cru[
        (df_fb_campaigns_cru["Objective"] == "OUTCOME_SALES")
        | (df_fb_campaigns_cru["Objective"] == "CONVERSIONS")
    ]

    # Filtar apenas campanhas de varejo ecommerce (drop atacado)

    df_fb_campaigns_filtrado = df_fb_campaigns_filtrado[
        ~df_fb_campaigns_filtrado.apply(
            lambda row: row.astype(str).str.contains("atacado").any(), axis=1
        )
    ]

    # Somar valor total investido "Spend"
    resultado_fb_spend_total = df_fb_campaigns_filtrado["Spend"].sum()

    # Somar valor total de impressões "Impressions"
    resultado_fb_impressions_total = df_fb_campaigns_filtrado[
        "Impressions"
    ].sum()

    # Somar valor total de cliques no link "Link Clicks"
    resultado_fb_linkclicks_total = df_fb_campaigns_filtrado[
        "Link Clicks"
    ].sum()

    # Somar valor total de venda "web_in_store_purchase"
    resultado_fb_vendas_total = df_fb_campaigns_filtrado[
        "web_in_store_purchase"
    ].sum()

    # Calcular CPM ("spend"/"Impressions"*1000)
    resultado_fb_cpm = (
        resultado_fb_spend_total / resultado_fb_impressions_total * 1000
    )

    # Calcular CPC ("spend"/"Link Clicks")
    resultado_fb_cpc = resultado_fb_spend_total / resultado_fb_linkclicks_total

    # Calcular CTR ("Link Clicks" / "Impressions")
    resultado_fb_ctr = (
        resultado_fb_linkclicks_total / resultado_fb_impressions_total
    )

    # Calcular ROAS ("web_in_store_purchase" / "Spend")
    resultado_fb_roas = resultado_fb_vendas_total / resultado_fb_spend_total

    # Criar DF final de fb
    df_relger_fb_final = pd.DataFrame(
        {
            "DATA": [dataname1],
            "INVESTIDO": [resultado_fb_spend_total],
            "VALOR VENDA FB": [resultado_fb_vendas_total],
            "ROAS": [resultado_fb_roas],
            "CPM": [resultado_fb_cpm],
            "CPC": [resultado_fb_cpc],
            "CTR": [resultado_fb_ctr],
            "IMPRESSÕES": [resultado_fb_impressions_total],
            "CLIQUES NO LINK": [resultado_fb_linkclicks_total],
        }
    )

    # Replace NaN values with 0
    df_relger_fb_final = df_relger_fb_final.fillna(0)

    # Replace Infinity values with 0
    df_relger_fb_final.replace([np.inf, -np.inf], 0, inplace=True)

    # In[03]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    # email: str = os.environ.get("supabase_email")
    # password: str = os.environ.get("supabase_password")

    # data = Client.sign_in(
    #     {"email": email, "password": password}
    # )

    dic_relger_fb_final = df_relger_fb_final.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"df_relger_fb_{client}")
            .upsert(dic_relger_fb_final)
            .execute()
        )

    except Exception as exception:
        print(exception)
