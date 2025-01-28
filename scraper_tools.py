import requests
import json
from shapely.geometry import Point
from geopandas.tools import geocode
import pandas as pd
from dotenv import load_dotenv
import os

# Variáveis de ambiente
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

# Buscador de cep com Viacep api
def busca_cep(endereco):
    endereco = endereco.split("/")[0]
    print(f"o endereço usado é {endereco}")

    try:
        response = requests.get(f"https://viacep.com.br/ws/DF/Brasilia/{endereco}/json/")
        
        if response.status_code == 200:
            saida_json = json.loads(response.text)
            if len(saida_json) > 0:
                cep = int(saida_json[0]["cep"].replace("-", ""))
                print(f"O CEP é {cep}\n======================")
                return cep
            else:
                print("Erro na busca cep (Endereço não encontrado)\n======================")
                return
        else:
            print("Erro na busca cep (Bad request)\n======================")
            return
    except:
        print("Erro na busca cep (Timeout)\n==========================")
        return

# Geocoding com google api
def google_geocoding(endereco, chave_api):
    endereco = endereco.replace(" ", "+")
    response = requests.get(fr"https://maps.googleapis.com/maps/api/geocode/json?address={endereco}&key={chave_api}")
    text = json.loads(response.text)

    lat = text["results"][0]['geometry']['location']['lat']
    lon = text["results"][0]['geometry']['location']['lng']

    geom = Point([lon, lat])

    return geom

# Geocodificação
def busca_geometria(pd_row):
    # Tenta buscar no Nominatim a partir do cep
    if pd.notnull(pd_row['cep']):
        resultados = geocode(pd_row['cep'], provider='nominatim', user_agent="myGeocoder")
        
        if not resultados.empty:
            print("CEP localizado. Usando Nominatim...")
            return resultados.loc[0, "geometry"]
        else:
            print("CEP não localizado. Usando Google Geocoding...")
            geom = google_geocoding(pd_row['endereco'], chave_api=api_key)
            return geom

    # Busca no Google a partir do endereço
    else:
        print("CEP é None. Usando Google Geocoding...")
        geom = google_geocoding(pd_row['endereco'], chave_api=api_key)
        return geom