# API DREAM V.1

import requests
import pandas as pd

url = "https://api.magedream.com.br/public/products/photos"

headers = {
    "Authorization": "Bearer m5YEjOn7wgEQPcbtoFSzWvZjvrIOJ8gQ468OXJme",
    "x-store": "lojahaut",
}
params = {"size": 100, "page": 1}

response = requests.get(url, headers=headers, params=params)

# Check response
if response.status_code == 200:
    data = response.json()  # Parse JSON response
    print(data)
else:
    print(f"Error: {response.status_code}")
