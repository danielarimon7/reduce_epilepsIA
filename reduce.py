import pika
import json

# IP de la VM donde está RabbitMQ
rabbitmq_ip = '10.128.0.5'

# Conexión al RabbitMQ usando la IP directamente
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip))
channel = connection.channel()

# Cola desde donde el Reducer va a leer
channel.queue_declare(queue='reduce_queue')

# Lista donde se acumulan los datos recibidos
resultados = []

def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Reduce recibió:", data)
    
    # Agrega la respuesta al acumulador
    resultados.append(data)
    
    # Ejemplo: reducir después de recibir 5 respuestas
    if len(resultados) >= 5:
        print("Reduciendo datos...")
        print(json.dumps(resultados, indent=4))
        resultados.clear()  # Limpiar si se va a seguir procesando

# Asociar el callback
channel.basic_consume(queue='reduce_queue', on_message_callback=callback, auto_ack=True)

print('Esperando mensajes desde RabbitMQ...')
channel.start_consuming()
