# ============================================================
# src/transformer.py
# Módulo de transformación de datos.
#
# Responsabilidades:
#   - Cargar el CSV con las columnas seleccionadas
#   - Limpiar y validar cada campo del payload
#   - Calcular campos derivados (duration_min, release_year)
#   - Calcular el Índice de Fiesta (métrica analítica custom)
#
# No tiene dependencias de MQTT ni de la base de datos:
# solo recibe un dict y devuelve otro. Fácil de testear.
# ============================================================

import re
import logging
import pandas as pd
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
import src.config as config

logger = logging.getLogger(__name__)

# ============================================================
# ÍNDICE DE FIESTA — Definición y pesos
# ============================================================
# Métrica compuesta que mide el potencial de una canción
# para animar una fiesta. Combinación lineal ponderada de
# tres características de audio de Spotify, todas en [0, 1].
#
# Justificación de pesos (para la defensa):
#   · Danceability (40%): El factor más determinante. Una canción
#     puede ser energética pero no bailable (ej. heavy metal).
#   · Energy (35%): Intensidad y actividad percibida. Canciones
#     de fiesta suelen ser rápidas y con mucho ruido.
#   · Valence (25%): Positividad musical. Una canción triste puede
#     ser bailable, así que pesa menos que las otras dos.
#
# Resultado: valor entre 0.0 (nada de fiesta) y 1.0 (fiesta total).
# ============================================================
FIESTA_WEIGHTS = {
    "danceability": 0.40,
    "energy":       0.35,
    "valence":      0.25,
}


# ============================================================
# Funciones de utilidad para conversión segura de tipos
# ============================================================

def safe_float(value, min_val: float = 0.0, max_val: float = 1.0) -> Optional[float]:
    """
    Convierte a float y valida que esté dentro del rango [min_val, max_val].
    Retorna None si la conversión falla o el valor está fuera de rango.
    """
    try:
        f = float(value)
        if min_val <= f <= max_val:
            return f
        return None
    except (ValueError, TypeError):
        return None


def safe_int(value, min_val: int = None, max_val: int = None) -> Optional[int]:
    """
    Convierte a int con validación de rango opcional.
    Retorna None si falla o si el valor está fuera de rango.
    """
    try:
        i = int(float(value))
        if min_val is not None and i < min_val:
            return None
        if max_val is not None and i > max_val:
            return None
        return i
    except (ValueError, TypeError):
        return None


def extract_year_from_text(text: str) -> Optional[int]:
    """
    Intenta extraer un año de 4 dígitos (rango 1950–2025)
    del nombre de un álbum u otro campo de texto.

    Ejemplos que detecta:
        "Greatest Hits 1999"  → 1999
        "Live at Wembley (2003)" → 2003
        "Vol. 2 [2012 Remaster]" → 2012

    Nota sobre el dataset de Kaggle:
    La columna 'release_date' no existe en todos los datasets de Spotify.
    Usamos el nombre del álbum como heurística. Si no se encuentra año,
    la columna release_year quedará NULL (los agregados temporales
    solo incluirán los tracks con año conocido).
    """
    if not isinstance(text, str):
        return None
    # Busca años de 4 dígitos en rango 1950-2025
    matches = re.findall(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    return int(matches[0]) if matches else None


# ============================================================
# Función principal de transformación
# ============================================================

def calculate_fiesta_index(danceability: float, energy: float, valence: float) -> float:
    """
    Calcula el Índice de Fiesta como combinación lineal ponderada.

    Args:
        danceability: Valor Spotify en [0, 1]
        energy:       Valor Spotify en [0, 1]
        valence:      Valor Spotify en [0, 1]

    Returns:
        Índice de Fiesta en [0.0, 1.0], redondeado a 3 decimales.
    """
    return round(
        danceability * FIESTA_WEIGHTS["danceability"] +
        energy       * FIESTA_WEIGHTS["energy"] +
        valence      * FIESTA_WEIGHTS["valence"],
        3
    )


def transform_track(raw_payload: dict) -> Optional[dict]:
    """
    Transforma y valida un payload crudo recibido de MQTT.

    Proceso:
        1. Valida track_id (campo obligatorio — clave primaria).
        2. Limpia y trunca campos de texto.
        3. Convierte y valida campos numéricos con rangos de Spotify.
        4. Calcula duration_min (ms → minutos).
        5. Normaliza explicit a booleano.
        6. Intenta extraer release_year del nombre del álbum.
        7. Calcula el Índice de Fiesta si hay datos suficientes.

    Args:
        raw_payload: Diccionario Python tal como llegó del broker MQTT.

    Returns:
        Diccionario listo para upsert en gold_tracks,
        o None si el registro no supera la validación mínima.
    """

    # --- 1. Validación obligatoria: track_id ---
    track_id = raw_payload.get("track_id")
    if not track_id or not isinstance(track_id, str) or track_id.strip() == "":
        logger.warning("Registro descartado: track_id ausente o vacío.")
        return None

    # --- 2. Campos de texto (limpieza y truncado por seguridad) ---
    track_name  = str(raw_payload.get("track_name", "Unknown")).strip()[:500]
    artists     = str(raw_payload.get("artists",    "Unknown")).strip()[:500]
    album_name  = str(raw_payload.get("album_name", "")).strip()[:500]
    track_genre = str(raw_payload.get("track_genre","Unknown")).strip()[:100]

    # --- 3. Campos numéricos con rangos oficiales de Spotify ---
    popularity       = safe_int(raw_payload.get("popularity"),   0, 100)
    danceability     = safe_float(raw_payload.get("danceability"), 0.0, 1.0)
    energy           = safe_float(raw_payload.get("energy"),       0.0, 1.0)
    valence          = safe_float(raw_payload.get("valence"),      0.0, 1.0)
    tempo            = safe_float(raw_payload.get("tempo"),        0.0, 300.0)
    acousticness     = safe_float(raw_payload.get("acousticness"), 0.0, 1.0)
    instrumentalness = safe_float(raw_payload.get("instrumentalness"), 0.0, 1.0)
    speechiness      = safe_float(raw_payload.get("speechiness"),  0.0, 1.0)

    # --- 4. Duración: milisegundos → minutos ---
    duration_ms = safe_int(raw_payload.get("duration_ms"), 0)
    duration_min = round(duration_ms / 60000, 2) if duration_ms else None

    # --- 5. Explicit: normalizar a booleano ---
    explicit_raw = raw_payload.get("explicit", False)
    if isinstance(explicit_raw, bool):
        explicit = explicit_raw
    elif isinstance(explicit_raw, str):
        explicit = explicit_raw.lower() in ("true", "1", "yes")
    else:
        explicit = bool(explicit_raw) if explicit_raw is not None else False

    # --- 6. Año de lanzamiento (heurística sobre el nombre del álbum) ---
    release_year = extract_year_from_text(album_name)

    # --- 7. Índice de Fiesta (solo si tenemos los tres componentes) ---
    if all(v is not None for v in [danceability, energy, valence]):
        fiesta_index = calculate_fiesta_index(danceability, energy, valence)
    else:
        fiesta_index = None
        logger.debug(f"fiesta_index no calculado para '{track_name}': faltan componentes.")

    return {
        "track_id":         track_id.strip(),
        "track_name":       track_name,
        "artists":          artists,
        "album_name":       album_name,
        "track_genre":      track_genre,
        "release_year":     release_year,
        "popularity":       popularity if popularity is not None else 0,
        "danceability":     danceability,
        "energy":           energy,
        "valence":          valence,
        "tempo":            tempo,
        "acousticness":     acousticness,
        "instrumentalness": instrumentalness,
        "speechiness":      speechiness,
        "duration_min":     duration_min,
        "explicit":         explicit,
        "fiesta_index":     fiesta_index,
        "data_source":      raw_payload.get("data_source", "csv_kaggle"),
    }


def load_csv_as_records(csv_path: str, selected_columns: list) -> list:
    """
    Carga el CSV de Spotify y retorna una lista de diccionarios,
    filtrando solo las columnas de interés para el dashboard.

    Pandas se usa solo aquí: para leer el CSV eficientemente.
    El resto del pipeline trabaja con dicts de Python puros.

    Args:
        csv_path:         Ruta al archivo CSV.
        selected_columns: Lista de columnas a conservar.

    Returns:
        Lista de diccionarios (una entrada por fila del CSV).

    Raises:
        FileNotFoundError: Si el CSV no existe en la ruta indicada.
    """
    # usecols con lambda para no romper si falta alguna columna opcional
    df = pd.read_csv(
        csv_path,
        usecols=lambda c: c in selected_columns,
        low_memory=False,
    )

    original_count = len(df)

    # Eliminar duplicados de track_id (el dataset de Kaggle los tiene)
    if "track_id" in df.columns:
        df = df.drop_duplicates(subset=["track_id"])
        dropped = original_count - len(df)
        if dropped > 0:
            logger.info(f"Eliminados {dropped} tracks duplicados del CSV.")

    logger.info(f"CSV cargado correctamente: {len(df)} tracks únicos listos para enviar.")
    return df.to_dict(orient="records")
