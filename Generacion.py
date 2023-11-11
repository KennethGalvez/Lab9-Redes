import random
import json

def generar_temperatura():
    return round(random.uniform(0, 100), 2)

def generar_humedad():
    return random.randint(0, 100)

def generar_direccion_viento():
    direcciones = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
    return random.choice(direcciones)

# Generar datos
temperatura = generar_temperatura()
humedad = generar_humedad()
direccion_viento = generar_direccion_viento()

# Condensar datos en formato JSON
datos_sensor = {
    "temperatura": temperatura,
    "humedad": humedad,
    "direccion_viento": direccion_viento
}

json_datos = json.dumps(datos_sensor, indent=2)
print(json_datos)
