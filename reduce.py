import pika
import json

# IP de la VM donde está RabbitMQ
rabbitmq_ip = '10.128.0.16'  # Cambia esto por la IP real si es diferente

# Credenciales para acceder a RabbitMQ
credentials = pika.PlainCredentials('isis2503', 'isis2503')

# Establecer la conexión al broker
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_ip, credentials=credentials)
)
channel = connection.channel()

# Declarar la cola desde donde se reciben los mensajes
channel.queue_declare(queue='reduce_queue')

# Lista para acumular los resultados del reduce
resultados = []

# Función callback que se ejecuta cada vez que se recibe un mensaje
def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Reduce recibió:", data)
    resultados.append(data)

    # Ejemplo de reducción: mostrar cuando se acumulan 5 mensajes
    if len(resultados) >= 5:
        print("Reduciendo datos...")
        print(json.dumps(resultados, indent=4))
        resultados.clear()

# Suscribirse a la cola
channel.basic_consume(queue='reduce_queue', on_message_callback=callback, auto_ack=True)

print('Esperando mensajes desde RabbitMQ...')
channel.start_consuming()

