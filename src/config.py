# ============================================================
# src/config.py
# Configuración centralizada del pipeline.
# Todos los parámetros ajustables están aquí.
# ============================================================

# --- MQTT ---
MQTT_BROKER_HOST   = "localhost"
MQTT_BROKER_PORT   = 1883
MQTT_TOPIC         = "spotify/tracks"
MQTT_CLIENT_ID_PUB = "spotify_publisher"
MQTT_CLIENT_ID_SUB = "spotify_subscriber"
MQTT_QOS           = 1       # QoS 1 = entrega garantizada al menos una vez

# --- PostgreSQL ---
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "spotify_datalake"
DB_USER     = "datauser"
DB_PASSWORD = "datapass123"

# --- Pipeline ---
# Pausa entre mensajes en segundos (0.05 = ~20 tracks/seg).
# Súbelo a 0.5 si quieres ver el flujo más despacio en los logs.
PUBLISH_DELAY_SECONDS = 0.05

CSV_PATH = "datos/dataset.csv"

# Columnas del CSV original que conservamos para el dashboard.
# El resto se descarta para reducir ruido y coste de procesamiento.
SELECTED_COLUMNS = [
    "track_id",
    "track_name",
    "artists",
    "album_name",
    "track_genre",
    "popularity",        # 0-100 → eje principal de análisis de nichos
    "danceability",      # 0-1  → componente del Índice de Fiesta
    "energy",            # 0-1  → componente del Índice de Fiesta
    "valence",           # 0-1  → positividad musical, componente del Índice de Fiesta
    "tempo",             # BPM  → característica sónica para heatmaps
    "acousticness",      # 0-1  → evolución temporal (era acústica vs digital)
    "instrumentalness",  # 0-1  → diferencia géneros (clásica, jazz vs pop, rap)
    "speechiness",       # 0-1  → identifica rap/spoken word
    "duration_ms",       # ms   → convertiremos a minutos
    "explicit",          # bool → filtro adicional para el dashboard
]

# ============================================================
# API DE SPOTIFY — Credenciales y parámetros
# ============================================================
# Crear una app en: https://developer.spotify.com/dashboard
# Credenciales en: Settings → Client ID / Client Secret
# No requiere OAuth de usuario; usamos Client Credentials Flow
# (solo datos públicos: nuevos lanzamientos, charts, etc.)
# ============================================================

SPOTIFY_CLIENT_ID     = "TU_CLIENT_ID_AQUI"
SPOTIFY_CLIENT_SECRET = "TU_CLIENT_SECRET_AQUI"

# Álbumes de nuevos lanzamientos a consultar (máx. 50 por llamada)
SPOTIFY_NEW_RELEASES_LIMIT = 50

# Tope de tracks que capturamos por ejecución del publisher de API.
# Con 50 álbumes y ~10 tracks/álbum podríamos llegar a 500;
# este parámetro permite limitarlo para demos rápidas.
SPOTIFY_MAX_TRACKS = 200