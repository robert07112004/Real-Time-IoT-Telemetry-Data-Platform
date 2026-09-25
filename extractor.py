import requests
import time
import json
from kafka import KafkaProducer

url = "https://opensky-network.org/api/states/all?lamin=35.9468&lomin=-9.3928&lamax=43.7483&lomax=3.0394"
bootstrap_servers = ['localhost:19092']

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Iniciando extracción de telemetría... (Pulsa Ctrl+C para detener)")

try:
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            airplane_list = data.get('states', [])
            
            if airplane_list:
                print(f"Aviones detectados en la peninsula: {len(airplane_list)}")
                for airplane_details in airplane_list:
                    telemetry_data = {
                        'icao24':          airplane_details[0],
                        'callsign':        airplane_details[1],
                        'origin_country':  airplane_details[2],
                        'time_position':   airplane_details[3],
                        'last_contact':    airplane_details[4],
                        'longitude':       airplane_details[5],
                        'latitude':        airplane_details[6],
                        'baro_altitude':   airplane_details[7],
                        'on_ground':       airplane_details[8],
                        'velocity':        airplane_details[9],
                        'true_track':      airplane_details[10],
                        'vertical_rate':   airplane_details[11],
                        'sensors':         airplane_details[12],
                        'geo_altitude':    airplane_details[13],
                        'squawk':          airplane_details[14],
                        'spi':             airplane_details[15],
                        'position_source': airplane_details[16]
                    }
                    producer.send('flight_telemetry', value=telemetry_data)
            else:
                print(f"La API no devolvió aviones en esta zona")
        else:
            print(f"Error al conectar con la API. Código de estado: {response.status_code}")
            
        print("Esperamos 15 segundos...\n")
        time.sleep(15)
        
except KeyboardInterrupt:
    print("\nSeñal de apagado recibida. Deteniendo el extractor...")
finally:
    if 'producer' in locals():
        print("Enviando mensajes residuales del búfer y cerrando conexión...")
        producer.flush()  
        producer.close()
        print("Extractor apagado de forma segura.")