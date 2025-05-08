import pika
import json

# RabbitMQ connection settings
rabbit_host = '10.128.0.20'
rabbit_user = 'isis2503'
rabbit_password = '1234'

# Crear conexión de publicación
def _create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_host,
            credentials=pika.PlainCredentials(rabbit_user, rabbit_password)
        )
    )

_publish_conn = _create_connection()
_publish_channel = _publish_conn.channel()

def declare_queue(queue_name: str):
    _publish_channel.queue_declare(queue=queue_name, durable=True)

def publish(message: dict, queue: str = 'ia_requests') -> None:
    declare_queue(queue)
    _publish_channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  
    )
