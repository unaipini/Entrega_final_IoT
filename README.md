# Pipeline IoT de Spotify – Versión Combinada

Pipeline de datos en tiempo real usando MQTT, PostgreSQL, Airflow y Grafana.
Lee un dataset histórico de Spotify y nuevos lanzamientos de la API,
los transforma y los visualiza en dashboards en tiempo real.

## 📋 Requisitos previos

- **Docker** y **Docker Compose** instalados
- **Cuenta de Spotify Developer** (para obtener `CLIENT_ID` y `CLIENT_SECRET`)
- Un archivo CSV con datos históricos de Spotify (opcional, pero recomendado)

## Arquitectura

```
CSV / API Spotify
       │
    Producer (MQTT publisher)
       │
    Mosquitto (broker MQTT)
       │
    Consumer (MQTT subscriber)
       │
    ┌──┴──────────────────────────────┐
    │  Bronze (raw JSONB)              │
    │  (auditoría completa)            │
    └──────────────────────────────────┘
       │  transform + validar
    ┌──┴──────────────────────────────────────────────┐
    │  Gold normalizado                               │
    │  • artists / albums / tracks                    │
    │  • audio_features (party_index GENERATED)       │
    │  • gold_genre_stats (stats por género)          │
    │  • gold_temporal_trends (evolución temporal)    │
    └──────────────────────────────────────────────────┘
       │
    Grafana (dashboard) + Airflow (ETL cada hora)
```

## Stack

| Servicio   | Rol                                      |
|------------|------------------------------------------|
| Mosquitto  | Broker MQTT (QoS 1)                      |
| PostgreSQL | BD con schema Bronze + Gold normalizado  |
| Grafana    | Dashboard provisionado automáticamente   |
| Airflow    | Orquestación ETL horaria con reintentos  |
| Producer   | Publica CSV + API Spotify en MQTT        |
| Consumer   | Recibe MQTT, guarda Bronze y Gold        |

## Inicio rápido

### 1. Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` con:

- **SPOTIFY_CLIENT_ID** y **SPOTIFY_CLIENT_SECRET**: obtener en https://developer.spotify.com/dashboard
- **AIRFLOW__CORE__FERNET_KEY**: generar con:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

### 2. Preparar dataset (opcional)

```bash
cp /ruta/al/dataset.csv datos/dataset.csv
```

Si no hay CSV, el Producer igual funciona extrayendo de la API de Spotify.

### 3. Levantar servicios

```bash
docker compose up -d
```

### 4. Esperar a que PostgreSQL esté listo

```bash
sleep 15
docker compose logs -f consumer producer
```

## Servicios expuestos

| URL                      | Servicio    | Usuario | Contraseña |
|--------------------------|-------------|---------|-----------|
| http://localhost:3000    | Grafana     | admin   | admin123  |
| http://localhost:8080    | Airflow     | admin   | admin123  |
| localhost:5432           | PostgreSQL  | (ver .env)           |
| localhost:1883           | Mosquitto   | (anónimo)            |

## Queries útiles para Grafana

```sql
-- Top 20 canciones por party_index
SELECT track_name, artist_name, party_index, popularity, source
FROM vw_top_party_tracks LIMIT 20;

-- Evolución temporal de danceability y energy
SELECT release_year, avg_danceability, avg_energy, avg_party_index
FROM gold_temporal_trends
ORDER BY release_year;

-- Géneros con mayor índice de fiesta
SELECT track_genre, avg_party_index, track_count
FROM gold_genre_stats
ORDER BY avg_party_index DESC LIMIT 10;

-- Comparativa CSV vs API
SELECT source, COUNT(*) AS total, ROUND(AVG(popularity), 1) AS avg_popularity
FROM tracks GROUP BY source;

-- Auditoría: mensajes en Bronze que no pasaron a Gold
SELECT COUNT(*) AS en_bronze,
       (SELECT COUNT(*) FROM tracks) AS en_gold,
       COUNT(*) - (SELECT COUNT(*) FROM tracks) AS descartados
FROM bronze_raw;
```

## party_index

Métrica propia del pipeline. No viene de Spotify.

```
party_index = danceability × 0.40 + energy × 0.35 + valence × 0.25
```

**Pesos justificados:**
- **Danceability (40%)**: factor más determinante. Una canción puede ser energética pero no bailable.
- **Energy (35%)**: intensidad y actividad percibida.
- **Valence (25%)**: positividad musical. Una canción triste puede ser bailable, pesa menos.

En PostgreSQL está definido como columna `GENERATED ALWAYS AS` en `audio_features`,
lo que garantiza que el cálculo es siempre correcto aunque se inserte por otra vía.

## Tests

```bash
pip install pytest
pytest tests/ -v --tb=short
```

## Pre-commits

```bash
pip install pre-commit
pre-commit install
```

Luego, **Black**, **isort**, **Flake8**, **Bandit** y **Gitleaks** se ejecutan automáticamente en cada commit.

## Detener servicios

```bash
docker compose down
```

Para limpiar volúmenes (reset total):

```bash
docker compose down -v
```
