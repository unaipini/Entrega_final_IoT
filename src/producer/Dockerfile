# src/producer/Dockerfile
# Imagen del servicio productor MQTT.
# Usa Python 3.11 slim para mantener la imagen ligera.

FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias primero para aprovechar la caché de capas de Docker.
# Si solo cambia el código Python, esta capa no se vuelve a construir.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente del productor
COPY main.py .
COPY spotify_client.py .

# El CSV se monta como volumen en docker-compose.yml (/app/data)
# No se copia aquí para evitar duplicar los datos en la imagen.

CMD ["python", "main.py"]
