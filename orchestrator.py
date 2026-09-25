import subprocess
import time
import sys
from datetime import datetime

def run_batch_pipeline():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n[{current_time}] Empieza a ejecutarse el script de la capa Silver")
    try:
        subprocess.run([sys.executable, "silver.py"], check=True)
        print(f"[{current_time}] Ha terminado el script de la capa Silver con éxito")
    except subprocess.CalledProcessError as e:
        print(f"[{current_time}] ERROR: Ha fallado la capa Silver debido a: {e}")
        print("Abortando la ejecución de la capa Gold por fallo en origen.")
        return  
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Empieza a ejecutarse el script de la capa Gold")
    try:
        subprocess.run([sys.executable, "gold.py"], check=True)
        print(f"[{current_time}] Ha terminado el script de la capa Gold con éxito")
    except subprocess.CalledProcessError as e:
        print(f"[{current_time}] ERROR: Ha fallado la capa Gold debido a: {e}")

if __name__ == "__main__":
    print("Iniciando Orquestador Batch (Silver -> Gold)")
    while True:
        run_batch_pipeline()
        print("\nSiguiente refresco dentro de 15 minutos...")
        time.sleep(900)