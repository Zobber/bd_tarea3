from kafka import KafkaProducer
import time
import json
import random

# Esta es la configuración del Producer de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Cambien la Ip si no está en local, 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Esto genera y envía datos en tiempo real
while True:
    # Crear un mensaje con datos aleatorios para probar lo que sería como un evento realista
    data = {
        'event': 'page_view',
        'user_id': random.randint(1, 100),
        'timestamp': time.time()
    }
    # Enviar el mensaje al topic 'my-topic'
    producer.send('my-topic', value=data)
    print(f"Data sent: {data}")
    time.sleep(1)  # Pausar 1 segundo entre cada mensaje
