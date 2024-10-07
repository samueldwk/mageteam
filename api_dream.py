# API DREAM V.1

import requests
import pandas as pd
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import os


url = "https://api.magedream.com.br/public/products/photos"
# url = "https://api.magedream.com.br/public/products/photos?page=2&size=100"

headers = {
    "Authorization": "Bearer m5YEjOn7wgEQPcbtoFSzWvZjvrIOJ8gQ468OXJme",
    "x-store": "tobstore",
}
params = {"size": 100, "page": 1}


def fetch_all_pages(url):
    all_results = []
    page_number = 1  # Track the page number

    while url:
        print(f"Fetching page: {page_number}")

        # For the first request, use params. For subsequent requests, do not use params
        if page_number == 1:
            response = requests.get(
                url, headers=headers, params=params
            )  # First request with params
        else:
            response = requests.get(
                url, headers=headers
            )  # Subsequent requests with full URL

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()

        # Append the data from the current page to all_results
        new_data = data.get("data", [])
        all_results.extend(new_data)

        # Check if new data was retrieved
        if not new_data:
            print("No new data found on this page.")
            break

        # Get the next page URL from the "_links" section
        next_url = data.get("_links", {}).get("next")
        print(f"Next URL: {next_url}")

        # If there's no next URL, stop the loop
        if not next_url:
            print("No next URL found, stopping pagination.")
            break

        # Update URL to next page, without using params for next requests
        url = next_url
        page_number += 1  # Increment the page number

    return all_results


# Fetch all pages
data = fetch_all_pages(url)

df_url_fotos = pd.DataFrame(data)

# SALVAR url_fotos como excel no computador

directory = (
    r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"
)

file_name = "url_fotos.xlsx"

# Define the full file path
file_path = os.path.join(directory, file_name)

# Save DataFrame to Excel file
df_url_fotos.to_excel(file_path, index=False)
