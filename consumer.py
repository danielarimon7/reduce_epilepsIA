import json
import pika
from reducer import reducir_respuestas

# Configuración de conexión
RABBIT_HOST = '10.128.0.20'
RABBIT_USER = 'isis2503'
RABBIT_PASSWORD = '1234'

# Establecer conexión con RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    )
)
channel = connection.channel()
channel.queue_declare(queue='reduce_queue', durable=True)

# Función que se ejecuta por cada mensaje recibido
def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    reducir_respuestas(mensaje)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Escuchar la cola
channel.basic_consume(queue='reduce_queue', on_message_callback=callback)
print("Esperando fragmentos desde RabbitMQ...")
channel.start_consuming()


