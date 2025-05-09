from collections import defaultdict, Counter
from typing import Dict
from db_client import persistir_resultado
from gcs_utils import eliminar_fragmentos_por_urls

# Estructura para almacenar fragmentos por examen
fragmentos_recibidos = defaultdict(dict)

def reducir_respuestas(mensaje: Dict) -> None:
    """
    Recibe un fragmento, lo acumula, y cuando están todos,
    realiza la reducción, guarda el resultado y elimina los fragmentos de GCS.
    """
    id_examen = mensaje['id_examen']
    num_fragmento = mensaje['num_fragmento']
    total_fragmentos = mensaje['total_fragmentos']

    # Guardar el fragmento recibido
    fragmentos_recibidos[id_examen][num_fragmento] = mensaje

    # Verificar si ya se recibieron todos los fragmentos
    if len(fragmentos_recibidos[id_examen]) == total_fragmentos:
        print(f" Todos los fragmentos de {id_examen} recibidos. Realizando reducción...")

        # Obtener los fragmentos ordenados por número
        fragmentos = [fragmentos_recibidos[id_examen][i] for i in range(1, total_fragmentos + 1)]

        # Unificar todas las respuestas
        respuestas_completas = []
        total_picos = 0
        for frag in fragmentos:
            respuestas_completas.extend(frag.get("respuestas", []))
            total_picos += frag.get("picos", 0)


        # Armar resultado final con total de picos
        resultado_final = {
            "id_examen": id_examen,
            "id_paciente": fragmentos[0]["id_paciente"],
            "total_picos": total_picos
        }

        # Enviar resultado a la API
        if persistir_resultado(resultado_final):
            # Eliminar fragmentos en GCS si el envío fue exitoso
            urls = [frag["ubicacion_fragmento"] for frag in fragmentos]
            eliminar_fragmentos_por_urls(urls)

            # Limpiar memoria
            del fragmentos_recibidos[id_examen]
            print(f" Proceso completo para {id_examen}.\n")
        else:
            print(f" Error al enviar resultado de {id_examen}. No se eliminan fragmentos todavía.")



