from google.cloud import storage
import gcs_settings
from typing import List

def eliminar_fragmentos_por_urls(urls: List[str]) -> None:
    """
    Elimina archivos en GCS a partir de sus URLs completas.
    
    :param urls: Lista de URLs de fragmentos subidos a GCS.
    """
    client = storage.Client()
    bucket = client.bucket(gcs_settings.GCS_BUCKET_NAME)
    prefix_url = f"https://storage.googleapis.com/{gcs_settings.GCS_BUCKET_NAME}/"

    for url in urls:
        if not url.startswith(prefix_url):
            print(f" URL inválida o fuera del bucket esperado: {url}")
            continue
        blob_path = url[len(prefix_url):]
        blob = bucket.blob(blob_path)
        blob.delete()
        print(f" Eliminado: {blob_path}")

    print("Todos los fragmentos eliminados correctamente.")
