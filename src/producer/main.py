"""
src/producer/main.py

Servicio productor MQTT del pipeline IoT de Spotify.

Este componente tiene dos responsabilidades principales que operan de forma concurrente:
1. Lectura e Ingestion del Dataset Historico: Procesa un archivo CSV local fila por fila,
   convierte cada entrada a un diccionario estandarizado e inyecta los datos en el broker
   MQTT utilizando la marca de origen 'csv'.
2. Monitorizacion en Tiempo Real (API): Ejecuta ciclos periodicos mediante el cliente
   de Spotify para extraer los ultimos lanzamientos musicales y publicarlos en el broker
   MQTT utilizando la marca de origen 'api'.

Ambas fuentes de datos convergen en el mismo topico MQTT ("spotify/tracks") y utilizan
variables de entorno para definir las credenciales y las rutas de red, eliminando
configuraciones estaticas en el codigo.
"""

import csv
import json
import logging
import os
import time

import paho.mqtt.client as mqtt

from spotify_client import stream_new_releases

# Configuracion del sistema de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

# Parametros de configuracion obtenidos desde el entorno
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "spotify/tracks")
CSV_PATH = "/app/data/dataset.csv"
CSV_INTERVAL = float(os.environ.get("CSV_PUBLISH_INTERVAL", 0.05))

# Intervalo de tiempo en segundos para consultar nuevos lanzamientos
API_CYCLE_INTERVAL = 3600


def _safe_float(row, key):
    """
    Convierte un valor especifico del diccionario a punto flotante de forma segura.

    Args:
        row (dict): Diccionario que representa la fila actual del dataset.
        key (str): Clave del diccionario a buscar.

    Returns:
        float o None: El valor numerico si la conversion es exitosa o si el campo existe,
                      de lo contrario devuelve None.
    """
    val = row.get(key)
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _safe_int(row, key):
    """
    Convierte un valor especifico del diccionario a numero entero de forma segura.

    Args:
        row (dict): Diccionario que representa la fila actual del dataset.
        key (str): Clave del diccionario a buscar.

    Returns:
        int o None: El valor numerico entero si la conversion es exitosa o si el campo
                    existe, de lo contrario devuelve None.
    """
    val = row.get(key)
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def _on_connect(client, userdata, flags, reason_code, properties=None):
    """
    Callback asincrono invocado cuando el cliente recibe una respuesta de conexion del broker MQTT.

    Verifica el codigo de retorno para confirmar que la conexion se realizo sin errores.
    """
    if reason_code == 0:
        logger.info("Conectado al broker MQTT en %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        logger.error("Fallo de conexion al broker MQTT. Codigo de error: %s", reason_code)


def publish(client: mqtt.Client, payload: dict) -> None:
    """
    Serializa y publica un mensaje en el broker MQTT.

    Garantiza que la codificacion se realice en formato JSON valido, preservando los
    caracteres especiales mediante ensure_ascii=False. Utiliza QoS 1 para asegurar
    que el mensaje sea entregado al menos una vez al broker.

    Args:
        client (mqtt.Client): Instancia activa del cliente MQTT.
        payload (dict): Diccionario con los datos que se enviaran como mensaje.
    """
    message = json.dumps(payload, ensure_ascii=False, default=str)
    result = client.publish(MQTT_TOPIC, message, qos=1)
    result.wait_for_publish(timeout=5)
    logger.debug(
        "Mensaje publicado: %s [Origen: %s]", payload.get("track_name"), payload.get("data_source")
    )


def publish_csv(client: mqtt.Client) -> None:
    """
    Inicia la lectura progresiva del dataset historico (CSV) y lo inyecta en el sistema.

    El proceso sigue estos pasos:
    1. Verifica la existencia fisica del archivo.
    2. Lee fila por fila utilizando DictReader.
    3. Mapea, extrae y convierte los tipos de datos a un formato homogeneo.
    4. Envia el payload resultante al broker MQTT con un retraso configurado para no
       saturar la red y permitir el procesamiento en el extremo receptor.

    Args:
        client (mqtt.Client): Instancia activa del cliente MQTT.
    """
    if not os.path.exists(CSV_PATH):
        logger.warning("Archivo CSV historico no encontrado en %s. Se omitira su publicacion.", CSV_PATH)
        return

    logger.info("Iniciando publicacion del dataset historico CSV desde %s", CSV_PATH)
    count = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            payload = {
                "track_id": row.get("track_id") or row.get("id", ""),
                "track_name": row.get("track_name") or row.get("name", ""),
                "artist_id": row.get("artists", ""),
                "artist_name": row.get("artists", ""),
                "album_id": row.get("album_id", ""),
                "album_name": row.get("album_name") or row.get("album", ""),
                "track_genre": row.get("track_genre", "Unknown"),
                "duration_ms": _safe_int(row, "duration_ms"),
                "explicit": row.get("explicit", "False").lower() == "true",
                "popularity": _safe_int(row, "popularity"),
                "danceability": _safe_float(row, "danceability"),
                "energy": _safe_float(row, "energy"),
                "valence": _safe_float(row, "valence"),
                "tempo": _safe_float(row, "tempo"),
                "acousticness": _safe_float(row, "acousticness"),
                "instrumentalness": _safe_float(row, "instrumentalness"),
                "speechiness": _safe_float(row, "speechiness"),
                "loudness": _safe_float(row, "loudness"),
                "data_source": "csv",
            }
            publish(client, payload)
            count += 1
            if count % 500 == 0:
                logger.info("Progreso CSV: %d pistas publicadas en el broker.", count)
            time.sleep(CSV_INTERVAL)

    logger.info("Finalizada la publicacion del CSV: Un total de %d pistas inyectadas.", count)


def publish_api(client: mqtt.Client) -> None:
    """
    Ejecuta el ciclo de consulta hacia la API de Spotify y publica los resultados.

    Realiza una peticion para recuperar los ultimos lanzamientos, inyectando la
    informacion extraida en el broker MQTT de la misma manera que el flujo CSV.
    Cada evento emitido es marcado explícitamente con el origen 'api'.

    Args:
        client (mqtt.Client): Instancia activa del cliente MQTT.
    """
    logger.info("Iniciando ciclo de extraccion de nuevos lanzamientos desde la API de Spotify.")
    try:
        for track in stream_new_releases(limit=20):
            track["data_source"] = "api"
            publish(client, track)
            time.sleep(1)
    except Exception as exc:
        logger.error("Se produjo un error al extraer datos interactivos de Spotify: %s", exc)


def main() -> None:
    """
    Punto de entrada principal del servicio productor.

    Maneja la inicializacion del cliente MQTT y coordina los bucles de ejecucion
    asincronos:
    1. Establece conexion persistente con el broker y define politicas de reintento en caso
       de no estar disponible la infraestructura.
    2. Dispara la publicacion secuencial del dataset historico en segundo plano.
    3. Mantiene un bucle infinito en el hilo principal que despierta cada hora (configurable)
       para solicitar nuevos datos frescos desde la API de Spotify y evitar que el pipeline quede inactivo.
    """
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_producer")
    client.on_connect = _on_connect

    # Intento constante de conexion en caso de fallos iniciales de la red
    connected = False
    while not connected:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            connected = True
        except OSError:
            logger.warning("El broker MQTT no se encuentra disponible. Reintentando la conexion en 5 segundos...")
            time.sleep(5)

    # Inicializa el hilo de red que maneja de forma concurrente el trafico entrante/saliente
    client.loop_start()

    # Ejecucion inicial: Carga de todo el historico masivo del archivo
    publish_csv(client)

    # Ejecucion persistente: Consulta de API para inyectar datos esporadicos nuevos
    last_api_run = 0.0
    while True:
        now = time.time()
        if now - last_api_run >= API_CYCLE_INTERVAL:
            publish_api(client)
            last_api_run = time.time()
        time.sleep(60)


if __name__ == "__main__":
    main()