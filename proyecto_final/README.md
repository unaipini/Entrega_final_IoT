# Pipeline IoT de Spotify — Versión Combinada

Pipeline de datos en tiempo real usando MQTT, PostgreSQL, Airflow y Grafana.
Lee un dataset histórico de Spotify y nuevos lanzamientos de la API,
los transforma y los visualiza en dashboards en tiempo real.

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
    ┌──┴──────────────────┐
    │  Bronze (raw JSONB) │  ← auditoría completa de lo que llegó
    └──────────────────────┘
       │  transform + validar
    ┌──┴──────────────────────────────────────┐
    │  Gold normalizado                        │
    │  artists / albums / tracks /             │
    │  audio_features (party_index GENERATED)  │
    │  gold_genre_stats (pre-calculado)        │
    │  gold_temporal_trends (pre-calculado)    │
    └──────────────────────────────────────────┘
       │
    Grafana (dashboard provisionado) + Airflow ETL (cada hora)
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

```bash
# 1. Copiar y rellenar variables de entorno
cp .env.example .env

# 2. Copiar el CSV en datos/
cp /ruta/al/spotify_tracks.csv datos/

# 3. Levantar todo
docker compose up -d

# 4. Ver logs del consumer
docker compose logs -f consumer
```

## Servicios expuestos

| URL                      | Servicio         | Credenciales (.env)     |
|--------------------------|------------------|-------------------------|
| http://localhost:3000    | Grafana          | GRAFANA_USER/PASSWORD   |
| http://localhost:8080    | Airflow          | AIRFLOW_ADMIN_USER/PASS |
| localhost:5432           | PostgreSQL       | POSTGRES_USER/PASSWORD  |

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

Pesos justificados:
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

## Pre-commits y CI

```bash
pip install pre-commit
pre-commit install
# A partir de aquí, Black, isort, Flake8, Bandit y Gitleaks
# se ejecutan automáticamente en cada commit.
```

El workflow de GitHub Actions ejecuta lint → tests → build Docker en cada push.
