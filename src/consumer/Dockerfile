# src/consumer/Dockerfile
# Imagen del servicio consumidor MQTT.

FROM python:3.11-slim

WORKDIR /app

# Dependencias primero para aprovechar la caché de capas Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código fuente del consumidor
COPY main.py .

CMD ["python", "main.py"]
