from confluent_kafka import Producer
import json
import time
import random

# Configuración del servidor Kafka 
bootstrap_servers = 'lab9.alumchat.xyz:9092'

# Configuración del Producer
conf = {'bootstrap.servers': bootstrap_servers}

# Instancia del Producer
producer = Producer(conf)

# Generar datos en formato JSON
def generar_data():
    temperatura = round(random.uniform(0, 100), 2)
    humedad = random.randint(0, 100)
    direccion_viento = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])
    
    data = {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }
    
    return json.dumps(data)

# Enviar datos al servidor Kafka
def enviar_datos_al_kafka():
    topic = '20079'  # carné 
    key = 'sensor1'
    
    try:
        while True:
            data = generar_data()
            producer.produce(topic, key=key, value=data)
            producer.flush()  # Asegurarse de que los mensajes se envíen
            print(f"Datos enviados al topic '{topic}': {data}")
            
            # Esperar entre 15 y 30 segundos antes de enviar el próximo lote de datos
            time.sleep(random.uniform(15, 30))
            
    except KeyboardInterrupt:
        # Interrupción de teclado para detener la ejecución
        print("Interrupción de teclado. Deteniendo el envío de datos.")
        producer.flush()

if __name__ == "__main__":
    enviar_datos_al_kafka()
