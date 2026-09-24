import requests
import time

url = "https://opensky-network.org/api/states/all?lamin=35.9468&lomin=-9.3928&lamax=43.7483&lomax=3.0394"

while True:

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            airplane_list = data.get('states', [])
            if airplane_list:
                print(f"Aviones detectados en la peninsula: {len(airplane_list)}")

                for airplane_details in airplane_list[:1]:
                    airplane = {
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
                    print(airplane)
            else:
                print(f"La API no devolvio aviones en esta zona")
        else:
            print(f"Error al conectar con la API. Código de estado: {response.status_code}")
    except Exception as e:
        print(f"Error de conexion: {e}")
    print("Esperamos 15 segundos...\n")
    time.sleep(15)