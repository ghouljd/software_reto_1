import sqlite3
from google.cloud import storage
import base64
import json

def pubsub_subscriber(event, context):
    try:
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            message = json.loads(message_data)
            
            # Ahora puedes acceder a cada campo del mensaje
            placa = message.get('placa')
            print(f"Timestamp: {message.get('timestamp')}")
            print(f"Latitude: {message.get('latitude')}")
            print(f"Longitude: {message.get('longitude')}")
            print(f"Velocity: {message.get('velocity')}")
            print(f"Direction: {message.get('direction')}")
            print(f"Temperature: {message.get('temperature')}")

            project_id = "gifted-pulsar-414313"

            # Nombre del archivo de la base de datos SQLite en Cloud Storage
            bucket_name = "experiment_bucket_software_gp3"
            blob_name = "rules_engine.db"

            # Descargar el archivo de la base de datos desde Cloud Storage
            storage_client = storage.Client(project=project_id)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename("/tmp/database.db")

            # Conectar a la base de datos SQLite
            conn = sqlite3.connect("/tmp/database.db")
            cursor = conn.cursor()  # Definir el cursor aquí

            # Ejecutar una consulta SQL
            cursor.execute("SELECT * FROM rules")

            # Obtener resultados
            results = cursor.fetchall()

            # Cerrar la conexión a la base de datos
            conn.close()

            print(f"Results: {str(results)}, Evento disparado por el vehiculo: {placa}")
        else:
            print("No data found in the Pub/Sub message.")
        
        print("Data not available")
    except:
        print("An exception has ocurred")
        raise