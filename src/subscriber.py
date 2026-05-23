# ============================================================
# src/subscriber.py
# Suscriptor MQTT + Procesador de datos
#
# Escucha los mensajes del broker, ejecuta el pipeline
# completo de datos para cada track recibido:
#
#   MQTT msg → Bronze (crudo) → transform → Gold (limpio)
#
# Además, cada REFRESH_EVERY_N mensajes, recalcula las
# tablas de agregados (genre_stats y temporal_trends)
# que el dashboard consume directamente.
#
# Uso:
#   cd src/
#   python subscriber.py     ← arrancar ANTES del publisher
#
# Prerequisitos:
#   - Docker Compose levantado (Mosquitto + PostgreSQL)
# ============================================================

import json
import logging
import signal
import sys
import os

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
import src.config as config
import src.db as db
from src.transformer import transform_track

# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SUBSCRIBER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================
# Estado global del subscriber
# ============================================================

# Recalcular agregados cada N tracks procesados exitosamente.
# Con 114k tracks y delay=0.05s, N=500 → refrescos cada ~25s.
REFRESH_EVERY_N = 500

# Contador de tracks procesados (compartido entre callbacks)
_message_counter = 0
_discarded_counter = 0


# ============================================================
# Lógica de procesamiento de mensajes
# ============================================================

def process_message(raw_payload: dict):
    """
    Ejecuta el pipeline completo para un mensaje recibido.

    Pasos:
        1. Bronze: insertar el JSON crudo tal como llegó.
        2. Transformar: limpiar, validar y enriquecer el payload.
        3. Gold: hacer upsert del track procesado.
        4. Cada REFRESH_EVERY_N, recalcular los agregados Gold.

    Args:
        raw_payload: Dict Python deserializado del mensaje MQTT.
    """
    global _message_counter, _discarded_counter

    # --- PASO 1: BRONZE — guardar dato crudo ---
    # Se guarda siempre, incluso si el track falla la validación.
    # Esto permite auditoría completa de lo que llegó al sistema.
    db.insert_bronze(raw_payload)

    # --- PASO 2: TRANSFORMACIÓN ---
    transformed = transform_track(raw_payload)

    if transformed is None:
        _discarded_counter += 1
        # No insertamos en Gold, pero el dato sí quedó en Bronze
        return

    # --- PASO 3: GOLD — guardar dato procesado ---
    db.upsert_gold_track(transformed)
    _message_counter += 1

    # --- PASO 4: AGREGADOS — refrescar tablas Gold periódicamente ---
    if _message_counter % REFRESH_EVERY_N == 0:
        logger.info(
            f"[{_message_counter} procesados | {_discarded_counter} descartados] "
            f"Refrescando tablas de agregados Gold..."
        )
        db.refresh_genre_stats()
        db.refresh_temporal_trends()


# ============================================================
# Callbacks del cliente MQTT
# ============================================================

def on_connect(client, userdata, flags, rc):
    """
    Se ejecuta al conectar al broker.
    Aquí es donde nos suscribimos al topic: si el broker
    nos desconecta y reconecta, paho vuelve a llamar a
    on_connect y la suscripción se restaura automáticamente.
    """
    if rc == 0:
        logger.info(
            f"Conectado al broker MQTT. "
            f"Suscribiéndose a '{config.MQTT_TOPIC}' con QoS {config.MQTT_QOS}..."
        )
        client.subscribe(config.MQTT_TOPIC, qos=config.MQTT_QOS)
    else:
        logger.error(f"Error de conexión al broker. Código: {rc}")
        sys.exit(1)


def on_message(client, userdata, msg):
    """
    Se ejecuta cada vez que llega un mensaje al topic suscrito.
    Es el punto de entrada de todos los datos del sistema.
    """
    try:
        raw_payload = json.loads(msg.payload.decode("utf-8"))
        process_message(raw_payload)
    except json.JSONDecodeError as e:
        logger.error(f"Payload JSON inválido ignorado: {e}")
    except Exception as e:
        # Capturamos cualquier error para no matar el subscriber
        logger.error(f"Error inesperado procesando mensaje: {e}", exc_info=True)


def on_disconnect(client, userdata, rc):
    """Se ejecuta al desconectarse del broker."""
    if rc != 0:
        logger.warning(f"Desconexión inesperada del broker (código {rc}). Reconectando...")
    else:
        logger.info("Desconectado del broker MQTT.")


def on_subscribe(client, userdata, mid, granted_qos):
    """Confirma que la suscripción al topic fue aceptada por el broker."""
    logger.info(f"Suscripción confirmada. QoS concedido: {granted_qos[0]}")
    logger.info("🎧 Esperando mensajes... (Ctrl+C para detener)")


# ============================================================
# Manejo de cierre limpio
# ============================================================

def _shutdown(client: mqtt.Client):
    """
    Realiza el cierre limpio del subscriber:
    - Actualiza los agregados Gold una última vez.
    - Desconecta el cliente MQTT.
    - Imprime resumen final.
    """
    logger.info("Realizando cierre limpio...")
    try:
        db.refresh_genre_stats()
        db.refresh_temporal_trends()
        logger.info(
            f"Resumen final: {_message_counter} tracks en Gold | "
            f"{_discarded_counter} descartados | "
            f"{_message_counter + _discarded_counter} en Bronze"
        )
    except Exception as e:
        logger.error(f"Error en limpieza final: {e}")
    finally:
        client.disconnect()


# ============================================================
# Punto de entrada
# ============================================================

def main():
    # 1. Verificar que el schema de la BD está listo
    logger.info("Verificando schema de la base de datos...")
    try:
        db.ensure_tables_exist()
    except Exception as e:
        logger.error(
            f"No se pudo conectar a PostgreSQL: {e}\n"
            "¿Está el Docker Compose levantado? Prueba: docker-compose up -d"
        )
        sys.exit(1)

    # 2. Crear y configurar el cliente MQTT
    client = mqtt.Client(client_id=config.MQTT_CLIENT_ID_SUB)
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe  = on_subscribe

    # 3. Registrar manejadores de señales para cierre limpio (Ctrl+C, kill)
    def signal_handler(sig, frame):
        _shutdown(client)
        sys.exit(0)

    signal.signal(signal.SIGINT,  signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 4. Conectar al broker
    try:
        client.connect(
            config.MQTT_BROKER_HOST,
            config.MQTT_BROKER_PORT,
            keepalive=60,
        )
    except ConnectionRefusedError:
        logger.error(
            f"No se pudo conectar al broker en "
            f"{config.MQTT_BROKER_HOST}:{config.MQTT_BROKER_PORT}.\n"
            "¿Está el Docker Compose levantado? Prueba: docker-compose up -d"
        )
        sys.exit(1)

    # 5. Iniciar el loop bloqueante
    # loop_forever() gestiona reconexiones automáticas y callbacks.
    # Se interrumpe al llamar a client.disconnect() o con KeyboardInterrupt.
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
