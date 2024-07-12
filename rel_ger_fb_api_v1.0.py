# rel_ger_fb_api_v1.0
# REL GER FACEBOOK BY API

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os

dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

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

c_list = ["paconcept"]


for client in c_list:
    # In[01]: Meta ADS API

    llt = os.getenv("fb_llt")

    url_fb_insights = f"https://graph.facebook.com/v19.0/{os.getenv(f'fb_act_{client}')}?fields=campaigns%7Binsights.time_range(%7B'since'%3A'{dataname1}'%2C'until'%3A'{dataname1}'%7D)%7Bobjective%2Cspend%2Caction_values%2Cimpressions%2Cinline_link_clicks%7D%7D&access_token={llt}"

    response_fb = requests.get(url_fb_insights)

    dic_cru_fb = response_fb.json()
    # print(dic_cru_fb)

    campaigns = dic_cru_fb["campaigns"]["data"]

    # Initialize a list to store the structured data
    structured_data = []

    # Extract and structure the data
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        insights = campaign.get("insights", {}).get("data", [])

        for insight in insights:
            row = {
                "Campaign ID": campaign_id,
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

                # if not action_values in structured_data:
                #     row[action_type] = {"web_in_store_purchase": 0}

            structured_data.append(row)

    # Create DataFrame
    df_fb_campaigns = pd.DataFrame(structured_data)

    # Replace None values with 0
    df_fb_campaigns = df_fb_campaigns.fillna(0)

    # If no "web_in_store_purchase" on df_fb_campaigns add column if value 0

    if "web_in_store_purchase" not in df_fb_campaigns.columns:
        df_fb_campaigns["web_in_store_purchase"] = 0

    # In[02]: Calcular os valores finais de facebook

    # CONVERT COLUMNS TYPE
    columns_to_convert = [
        "Spend",
        "Impressions",
        "Link Clicks",
        "web_in_store_purchase",
    ]
    df_fb_campaigns[columns_to_convert] = df_fb_campaigns[
        columns_to_convert
    ].astype(float)

    # Filtrar apenas campanhas de objetivo conversão / vendas
    df_fb_campaigns = df_fb_campaigns[
        df_fb_campaigns["Objective"] == "OUTCOME_SALES"
    ]

    # Somar valor total investido "Spend"
    resultado_fb_spend_total = df_fb_campaigns["Spend"].sum()

    # Somar valor total de impressões "Impressions"
    resultado_fb_impressions_total = df_fb_campaigns["Impressions"].sum()

    # Somar valor total de cliques no link "Link Clicks"
    resultado_fb_linkclicks_total = df_fb_campaigns["Link Clicks"].sum()

    # Somar valor total de venda "web_in_store_purchase"
    resultado_fb_vendas_total = df_fb_campaigns["web_in_store_purchase"].sum()

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

    # Replace None values with 0
    df_relger_fb_final = df_relger_fb_final.fillna(0)

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
