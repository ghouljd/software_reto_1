import sqlite3
from google.cloud import storage
import base64
import json

def pubsub_subscriber(event,context):
    try:
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            message = json.loads(message_data)
            
            # Log event data
            plate = message.get('placa')
            timestamp = message.get('timestamp')
            latitude = message.get('latitude')
            longitude = message.get('longitude')
            velocity = message.get('velocity')
            direction = message.get('direction')
            temperature = message.get('temperature')
            print(f"Event received for vehicle {plate}, timestamp={timestamp}, latitude={latitude}, longitude={longitude}, velocity={velocity}, direction={direction}, temperature={temperature}")

            # Cloud config
            project_id = "gifted-pulsar-414313"
            bucket_name = "experiment_bucket_software_gp3"
            blob_name = "CCS.db"

            # Download DB file from the storage cloud
            storage_client = storage.Client(project=project_id)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename("/tmp/database.db")

            # Open a connection to the database
            conn = sqlite3.connect("/tmp/database.db")
            cursor = conn.cursor()

            # SQL query
            cursor.execute(f"select evento, cond_criterio, cond_operador, cond_operando, accion from rules where placa = '{plate}'")
            rules_set = cursor.fetchall()
            print(rules_set)
            # Close the database connection
            conn.close()

            # Validating rules
            if not rules_set: 
                print(f"Rules not set for the plate: {plate}")
            else:
                def evaluate_max_velocity(velocity):
                    velocity_rule = next((rule for rule in rules_set if rule[0] == "MAX_VELOCITY"), None)
                    if int(velocity) >= int(velocity_rule[3]):
                        print(f"Velocity alert for vehicle: {plate} - {velocity_rule[4]}")
                    else:
                        print(f"Velocity according to the rule for the vehicle {plate}")
                    return
                def evaluate_min_temperature(temperature):
                    low_temperature_rule = next((rule for rule in rules_set if rule[0] == "TEMP_ANORMAL_DOWN"), None)
                    if int(temperature) <= int(low_temperature_rule[3]):
                        print(f"Low temperature alert for vehicle: {plate} - {low_temperature_rule[4]}")
                    else:
                        print(f"Low temperature according to the rule for the vehicle {plate}")
                    return
                def evaluate_max_temperature(temperature):
                    high_temperature_rule = next((rule for rule in rules_set if rule[0] == "TEMP_ANORMAL_UP"), None)
                    if int(temperature) >= int(high_temperature_rule[3]):
                        print(f"High temperature alert for vehicle: {plate} - {high_temperature_rule[4]}")
                    else:
                        print(f"High temperature according to the rule for the vehicle {plate}")
                    return
                evaluate_max_velocity(velocity)
                evaluate_min_temperature(temperature)
                evaluate_max_temperature(temperature)
        else:
            print("No data found in the Pub/Sub message.")
    except:
        print("An exception has ocurred")
        raise