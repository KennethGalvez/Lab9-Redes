from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt
import json

# Configuración del servidor Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '20079'  #mismo topic que el Producer
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

# Función para procesar el mensaje
def procesar_mensaje(mensaje):
    payload = json.loads(mensaje.value().decode('utf-8'))
    return payload

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

        payload = procesar_mensaje(mensaje)
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
