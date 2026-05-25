# airflow/Dockerfile
# Imagen de Airflow extendida con las dependencias propias del proyecto.
# Se construye sobre la imagen oficial de Apache Airflow 2.9 con Python 3.11.

FROM apache/airflow:2.9.1-python3.11

# Instalamos las dependencias adicionales del DAG:
#   - spotipy: cliente oficial de la API de Spotify
#   - psycopg2-binary: driver PostgreSQL para el hook de Airflow y el DAG
#   - paho-mqtt: cliente MQTT para publicar/consumir mensajes desde el DAG
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
