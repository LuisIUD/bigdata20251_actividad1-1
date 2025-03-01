import requests
import json

def obtener_datos_api(url="https://randomuser.me/api", params={}):
    """
    Obtiene datos de usuarios aleatorios desde la API RandomUser.
    
    Args:
        url (str): URL de la API.
        params (dict): Parámetros opcionales para la solicitud.
    
    Returns:
        dict: Datos obtenidos o diccionario vacío en caso de error.
    """
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(f"Error al obtener datos de la API: {error}")
        return {}
    
# Parámetros para la API (Ejemplo: obtener 5 usuarios)
parametros = {"results": 5}
url = "https://randomuser.me/api"

datos = obtener_datos_api(url=url, params=parametros)

if datos and "results" in datos:
    print(json.dumps(datos, indent=4))
else:
    print("No se pudieron obtener los datos de usuarios.")
