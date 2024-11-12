import requests
import pandas as pd
import dotenv
import os
import matplotlib.pyplot as plt


dotenv.load_dotenv()

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(url, key)

# SUPABASE API

response_db_fb = supabase.table("mage_fb_v1").select("*").execute()

df_db_fb = pd.DataFrame(response_db_fb.data)

# SALVAR EM EXCEL

directory = (
    r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"
)

file_name = "fb_db.xlsx"

# Define the full file path
file_path = os.path.join(directory, file_name)

# Save DataFrame to Excel file
df_db_fb.to_excel(file_path, index=False)


# # MONTAR UM GRÁFICO

# # Ensure the date column is in datetime format
# df_db_fb["data"] = pd.to_datetime(df_db_fb["data"])

# # Plotting the line chart
# plt.figure(figsize=(12, 6))

# # Plot each client's CPC trend over time
# for client in df_db_fb["mage_cliente"].unique():
#     client_data = df_db_fb[df_db_fb["mage_cliente"] == client]
#     plt.plot(
#         client_data["data"], client_data["fb_cpc"], marker="o", label=client
#     )

# # Chart labeling
# plt.xlabel("Date")
# plt.ylabel("CPC")
# plt.title("CPC Trend by Date and Mage Cliente")
# plt.legend(title="Mage Cliente")
# plt.xticks(rotation=45)
# plt.show()
