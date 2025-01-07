# CEP para coordenadas geográficas (latitude e longitude)

import brazilcep
from geopy.geocoders import Nominatim
import pandas as pd

# # R. Dragão do Mar, 81 - Praia de Iracema, Fortaleza - CE, 60060-390
# endereco = brazilcep.get_address_from_cep("60060390")

# geolocator = Nominatim(user_agent="test_app")
# location = geolocator.geocode(
#     endereco["street"]
#     + ", "
#     + endereco["city"]
#     + " - "
#     + endereco["district"]
# )

# print(location.latitude, location.longitude)


def buscar_coordenadas(lista_ceps):
    """
    Busca coordenadas geográficas (latitude e longitude) para uma lista de CEPs.

    Parâmetros:
    - lista_ceps (list): Lista de CEPs.

    Retorno:
    - DataFrame com as colunas CEP, latitude e longitude.
    """
    geolocator = Nominatim(user_agent="cep_geolocator_app")
    resultados = []

    for cep in lista_ceps:
        try:
            # Busca o endereço pelo CEP
            endereco = brazilcep.get_address_from_cep(cep)

            # Monta o endereço completo para geocodificação
            endereco_completo = f"{endereco['street']}, {endereco['city']} - {endereco['district']}"

            # Obtém as coordenadas (latitude e longitude)
            location = geolocator.geocode(endereco_completo)

            if location:
                resultados.append(
                    {
                        "CEP": cep,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                    }
                )
            else:
                # Caso o endereço não retorne coordenadas
                resultados.append(
                    {"CEP": cep, "latitude": None, "longitude": None}
                )

        except Exception as e:
            # Em caso de erro, adiciona valores nulos e continua
            print(f"Erro ao processar o CEP {cep}: {e}")
            resultados.append(
                {"CEP": cep, "latitude": None, "longitude": None}
            )

    # Converte os resultados em um DataFrame
    df_resultado = pd.DataFrame(resultados)
    return df_resultado


# CEP para Coordenadas

caminho_excel = r"C:\Users\Samuel Kim\Downloads\listacep.xlsx"  # Substitua pelo caminho do seu arquivo

# Ler o arquivo Excel
df_ceps = pd.read_excel(caminho_excel, dtype={"CEP": str})

# Transformar a coluna 'CEP' em uma lista
lista_ceps = df_ceps["CEP"].dropna().astype(str).tolist()

df_ceps = buscar_coordenadas(lista_ceps)
print(df_ceps)
