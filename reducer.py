from collections import defaultdict
from datetime import datetime
from typing import List, Dict

def reducir_respuestas(datos: List[Dict]) -> List[Dict]:
    conteo_respuestas = defaultdict(lambda: defaultdict(int))

    for d in datos:
        examen = d["numero_examen"]
        respuesta = d["respuesta"]
        conteo_respuestas[examen][respuesta] += 1

    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    salida = []
    for examen, respuestas in conteo_respuestas.items():
        for respuesta, conteo in respuestas.items():
            salida.append({
                "id": "reduce",
                "fecha": fecha_actual,
                "respuesta": respuesta,
                "numero_examen": examen,
                "conteo": conteo
            })
    return salida

