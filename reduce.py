import pika
import json
from collections import defaultdict
from datetime import datetime

rabbitmq_ip = '10.128.0.16'

credentials = pika.PlainCredentials('isis2503', 'isis2503')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_ip, credentials=credentials)
)
channel = connection.channel()

channel.queue_declare(queue='reduce_queue')

resultados = []

# Función de reducción con formato de mensaje original
def reducir_datos_formato_original(datos):
    conteo_respuestas = defaultdict(lambda: defaultdict(int))
    for d in datos:
        examen = d["numero_examen"]
        respuesta = d["respuesta"]
        conteo_respuestas[examen][respuesta] += 1

    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    resultados_reducidos = []
    for examen, respuestas in conteo_respuestas.items():
        for respuesta, conteo in respuestas.items():
            resultados_reducidos.append({
                "id": "reduce",
                "fecha": fecha_actual,
                "respuesta": respuesta,
                "numero_examen": examen,
                "conteo": conteo
            })

    return resultados_reducidos

# Callback cuando llega un mensaje
def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Reduce recibió:", data)
    resultados.append(data)

    if len(resultados) >= 5:
        print("\nReduciendo datos...\n")
        resultados_reducidos = reducir_datos_formato_original(resultados)
        for r in resultados_reducidos:
            print(json.dumps(r, indent=4))
        resultados.clear()

channel.basic_consume(queue='reduce_queue', on_message_callback=callback, auto_ack=True)

print('Esperando mensajes desde RabbitMQ...')
channel.start_consuming()
