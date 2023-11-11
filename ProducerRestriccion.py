from confluent_kafka import Producer
import json
import struct
import time
import random

# Configuración del servidor Kafka 
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '20079'  # mismo topic que el Consumer

# Configuración del Producer
conf = {'bootstrap.servers': bootstrap_servers}

# Crear instancia del Producer
producer = Producer(conf)

# Función para codificar el mensaje a bytes
def encode_mensaje(temperatura, humedad, direccion_viento):
    # Codificar temperatura a 14 bits
    temperatura_encoded = int((temperatura / 100.0) * (2**14 - 1))
    # Codificar humedad a 7 bits
    humedad_encoded = int((humedad / 100.0) * (2**7 - 1))
    # Codificar dirección del viento a 3 bits
    direccion_viento_encoded = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'].index(direccion_viento)

    # Empaquetar los datos en una estructura de bytes
    mensaje_encoded = struct.pack('>HBB', temperatura_encoded, humedad_encoded, direccion_viento_encoded)

    return mensaje_encoded

# Enviar datos al servidor Kafka
def enviar_datos_al_kafka():
    try:
        while True:
            # Generar datos simulados
            temperatura = round(25.5 + (random.uniform(-5, 5)), 2)
            humedad = random.randint(0, 100)
            direccion_viento = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])

            # Codificar mensaje a bytes
            mensaje_encoded = encode_mensaje(temperatura, humedad, direccion_viento)

            # Enviar mensaje al topic
            producer.produce(topic, value=mensaje_encoded)
            producer.flush()  # Asegurarse de que los mensajes se envíen

            print(f"Datos enviados al topic '{topic}': Temperatura={temperatura}, Humedad={humedad}, Dirección del Viento={direccion_viento}")
            
            # Esperar entre 15 y 30 segundos antes de enviar el próximo lote de datos
            time.sleep(random.uniform(15, 30))
            
    except KeyboardInterrupt:
        # Interrupción de teclado para detener la ejecución
        print("Interrupción de teclado. Deteniendo el envío de datos.")
        producer.flush()

if __name__ == "__main__":
    enviar_datos_al_kafka()

