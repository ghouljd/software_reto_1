import functions_framework
import sqlite3
from google.cloud import storage

from markupsafe import escape
@functions_framework.http
def hello_http(request):
    try:
        """HTTP Cloud Function.
        Args:
            request (flask.Request): The request object.
            <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
        Returns:
            The response text, or any set of values that can be turned into a
            Response object using `make_response`
            <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
        """
        project_id = "gifted-pulsar-414313"
        # request_json = request.get_json(silent=True)
        # request_args = request.args

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

        # if request_json and "name" in request_json:
        #     name = request_json["name"]
        # elif request_args and "name" in request_args:
        #     name = request_args["name"]
        # else:
        #     name = "World"
        return f"Results: {str(results)}"
    except:
        return "An exception has ocurred"
        raise