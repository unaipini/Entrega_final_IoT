# Pipeline de Datos — Ciclo de Vida del Dato 🎵
**Stack:** Python · MQTT (Eclipse Mosquitto) · PostgreSQL · Arquitectura Medallón

---

## Estructura del proyecto

```
spotify_pipeline/
│
├── docker-compose.yml          ← Levanta Mosquitto + PostgreSQL + pgAdmin
│
├── mosquitto/
│   └── config/mosquitto.conf   ← Configuración del broker MQTT
│
├── requirements.txt            ← Dependencias Python
│
├── datos/
│   └── dataset.csv      ← ⚠️ Descargar de Kaggle (ver abajo)
│
└── src/
    ├── config.py               ← Parámetros de conexión y constantes
    ├── db.py                   ← Conexión BD, creación de tablas, inserts
    ├── transformer.py          ← Limpieza, validación, Índice de Fiesta
    ├── publisher.py            ← Lee CSV y envía por MQTT (simulación)
    └── subscriber.py          ← Recibe MQTT, procesa y guarda en BD
```

---

## 1. Descargar el dataset

Dataset de Kaggle: **"Spotify Tracks Dataset"** (maharshipandya)
https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset

Renombrar el archivo descargado a `dataset.csv` y colocarlo en `datos/`.

---

## 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

---

## 3. Levantar la infraestructura con Docker

```bash
docker-compose up -d
```

Espera unos segundos a que PostgreSQL esté listo (healthcheck).
Puedes verificarlo con:

```bash
docker-compose ps
```

**pgAdmin** disponible en http://localhost:5050
- Email: admin@spotify.com
- Password: admin123
- Servidor: spotify_postgres · Puerto: 5432 · BD: spotify_datalake

---

## 4. Ejecutar el pipeline

**Terminal 1 — Arrancar el subscriber PRIMERO:**
```bash
cd src/
python subscriber.py
```

**Terminal 2 — Lanzar el publisher:**
```bash
cd src/
python publisher.py
```

El subscriber creará las tablas automáticamente en el primer arranque.
Verás los logs de progreso en ambas terminales.

---

## 5. Consultas SQL para el dashboard

Una vez terminado el pipeline, estas queries alimentan directamente
las visualizaciones en Power BI o Grafana.

### Heatmap: Danceability vs Energy por género
```sql
SELECT
    track_genre,
    ROUND(AVG(danceability), 3) AS avg_danceability,
    ROUND(AVG(energy), 3)       AS avg_energy,
    COUNT(*)                    AS n_tracks
FROM gold_tracks
GROUP BY track_genre
ORDER BY avg_energy DESC;
```

### Análisis de nichos: Popularidad media y Fiesta Index por género
```sql
SELECT
    track_genre,
    track_count,
    avg_popularity,
    avg_fiesta_index,
    avg_danceability,
    avg_energy
FROM gold_genre_stats
ORDER BY avg_popularity DESC;
```

### Evolución temporal: características sónicas por año
```sql
SELECT
    release_year,
    track_count,
    avg_danceability,
    avg_energy,
    avg_valence,
    avg_acousticness,
    avg_fiesta_index
FROM gold_temporal_trends
WHERE release_year >= 1960
ORDER BY release_year;
```

### Top tracks por Índice de Fiesta
```sql
SELECT
    track_name,
    artists,
    track_genre,
    popularity,
    fiesta_index,
    danceability,
    energy,
    valence
FROM gold_tracks
ORDER BY fiesta_index DESC
LIMIT 50;
```

### Volumen Bronze vs Gold (control de calidad del pipeline)
```sql
SELECT
    (SELECT COUNT(*) FROM bronze_raw)   AS total_raw,
    (SELECT COUNT(*) FROM gold_tracks)  AS total_gold,
    ROUND(
        100.0 * (SELECT COUNT(*) FROM gold_tracks) /
        NULLIF((SELECT COUNT(*) FROM bronze_raw), 0),
    1) AS pct_aprovechado;
```

---

## Índice de Fiesta — Fórmula

Métrica compuesta calculada en `transformer.py`:

```
Fiesta Index = (Danceability × 0.40) + (Energy × 0.35) + (Valence × 0.25)
```

- **Resultado:** entre 0.0 (nada de fiesta) y 1.0 (fiesta total)
- **Justificación de pesos:** Danceability es el factor más determinante;
  Energy añade intensidad; Valence (positividad) tiene menor peso porque
  una canción puede ser triste pero muy bailable.

---

## Arquitectura Medallón simplificada

| Capa | Tabla | Contenido |
|------|-------|-----------|
| 🥉 Bronze | `bronze_raw` | JSON crudo tal como llega de MQTT (JSONB) |
| 🥇 Gold | `gold_tracks` | Tracks limpios, normalizados y enriquecidos |
| 🥇 Gold | `gold_genre_stats` | Agregados por género (actualización continua) |
| 🥇 Gold | `gold_temporal_trends` | Tendencias por año de lanzamiento |
