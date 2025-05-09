import requests

# Nueva URL de la API para persistencia de resultados
API_URL = "https://34.59.118.140:8080/resultado/"

def persistir_resultado(resultado: dict) -> bool:
    """
    Envía el resultado reducido a la API externa para persistirlo en la base de datos.
    
    :param resultado: Diccionario con los campos finales a guardar.
    :return: True si el envío fue exitoso, False si falló.
    """
    try:
        response = requests.post(API_URL, json=resultado, timeout=10)
        response.raise_for_status()
        print(f" Resultado enviado correctamente: {resultado}")
        return True
    except requests.RequestException as e:
        print(f" Error al enviar resultado a la API: {e}")
        return False

