import pika
import json
from reducer import reducir_respuestas
from messaging import publish

# Configuración RabbitMQ
rabbit_host = '10.128.0.20'
rabbit_user = 'isis2503'
rabbit_password = '1234'

# Conexión
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=rabbit_host,
        credentials=pika.PlainCredentials(rabbit_user, rabbit_password)
    )
)
channel = connection.channel()

channel.queue_declare(queue='reduce_queue', durable=True)

buffer = []

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        print("Mensaje recibido:", data)
        buffer.append(data)

        if len(buffer) >= 5:
            print("Ejecutando reducción...")
            resultados = reducir_respuestas(buffer)
            for res in resultados:
                print(json.dumps(res, indent=4))
                publish(res, queue='final_output')  # o la cola que prefieras
            buffer.clear()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Error procesando mensaje:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='reduce_queue', on_message_callback=callback)
