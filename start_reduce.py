from consumer import channel

print("Reduce esperando mensajes en cola...")
channel.start_consuming()