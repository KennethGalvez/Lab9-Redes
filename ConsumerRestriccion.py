from confluent_kafka import Consumer, KafkaError
import struct
import matplotlib.pyplot as plt

# Configuración del servidor Kafka 
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '20079'  # mismo topic que el Producer
group_id = 'grupo9'

# Configuración del Consumer
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Para consumir desde el inicio del topic
}

# Crear instancia del Consumer
consumer = Consumer(conf)

# Suscribir al topic
consumer.subscribe([topic])

# Listas para almacenar datos para graficar
all_temp = []
all_hume = []
all_wind = []

# Función para decodificar el mensaje desde bytes a JSON
def decode_mensaje(mensaje_encoded):
    if len(mensaje_encoded) != 4:
        print("Error: Longitud de mensaje incorrecta.")
        return None
    
    # Desempaquetar los datos desde la estructura de bytes
    temperatura_encoded, humedad_encoded, direccion_viento_encoded = struct.unpack('>HBB', mensaje_encoded)

    # Decodificar temperatura de 14 bits
    temperatura = round((temperatura_encoded / (2**14 - 1)) * 100, 2)
    # Decodificar humedad de 7 bits
    humedad = round((humedad_encoded / (2**7 - 1)) * 100)
    # Decodificar dirección del viento de 3 bits
    direccion_viento = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'][direccion_viento_encoded]

    return {'temperatura': temperatura, 'humedad': humedad, 'direccion_viento': direccion_viento}

# Función para graficar los datos
def plot_all_data(temp, hume, wind):
    plt.plot(temp, label='Temperatura')
    plt.plot(hume, label='Humedad Relativa')
    plt.plot(wind, label='Dirección del Viento')
    plt.xlabel('Muestras')
    plt.ylabel('Valores')
    plt.legend()
    plt.show()

# Consumir y procesar mensajes
try:
    while True:
        mensaje = consumer.poll(1.0)  # Esperar mensajes durante 1 segundo
        if mensaje is None:
            continue
        if mensaje.error():
            if mensaje.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(mensaje.error())
                break

        # Decodificar mensaje desde bytes a JSON
        payload = decode_mensaje(mensaje.value())
        
        if payload is not None:
            # Almacenar datos para graficar
            all_temp.append(payload['temperatura'])
            all_hume.append(payload['humedad'])
            all_wind.append(payload['direccion_viento'])

            # Graficar datos actualizados
            plot_all_data(all_temp, all_hume, all_wind)

except KeyboardInterrupt:
    # Interrupción de teclado para detener la ejecución
    print("Interrupción de teclado. Deteniendo el consumo de datos.")

finally:
    # Cerrar el consumidor al finalizar
    consumer.close()
