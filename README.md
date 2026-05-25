# Pipeline IoT de Spotify â€” VersiÃ³n Combinada

Pipeline de datos en tiempo real usando MQTT, PostgreSQL, Airflow y Grafana.
Lee un dataset histÃ³rico de Spotify y nuevos lanzamientos de la API,
los transforma y los visualiza en dashboards en tiempo real.

## Arquitectura

```
CSV / API Spotify
       â”‚
    Producer (MQTT publisher)
       â”‚
    Mosquitto (broker MQTT)
       â”‚
    Consumer (MQTT subscriber)
       â”‚
    â”Œâ”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
    â”‚  Bronze (raw JSONB) â”‚  â† auditorÃ­a completa de lo que llegÃ³
    â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
       â”‚  transform + validar
    â”Œâ”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
    â”‚  Gold normalizado                        â”‚
    â”‚  artists / albums / tracks /             â”‚
    â”‚  audio_features (party_index GENERATED)  â”‚
    â”‚  gold_genre_stats (pre-calculado)        â”‚
    â”‚  gold_temporal_trends (pre-calculado)    â”‚
    â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
       â”‚
    Grafana (dashboard provisionado) + Airflow ETL (cada hora)
```

## Stack

| Servicio   | Rol                                      |
|------------|------------------------------------------|
| Mosquitto  | Broker MQTT (QoS 1)                      |
| PostgreSQL | BD con schema Bronze + Gold normalizado  |
| Grafana    | Dashboard provisionado automÃ¡ticamente   |
| Airflow    | OrquestaciÃ³n ETL horaria con reintentos  |
| Producer   | Publica CSV + API Spotify en MQTT        |
| Consumer   | Recibe MQTT, guarda Bronze y Gold        |

## Inicio rÃ¡pido

```bash
# 1. Copiar y rellenar variables de entorno
cp .env.example .env

# 2. Copiar el CSV en datos/
cp /ruta/al/dataset.csv datos/

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

## Queries Ãºtiles para Grafana

```sql
-- Top 20 canciones por party_index
SELECT track_name, artist_name, party_index, popularity, source
FROM vw_top_party_tracks LIMIT 20;

-- EvoluciÃ³n temporal de danceability y energy
SELECT release_year, avg_danceability, avg_energy, avg_party_index
FROM gold_temporal_trends
ORDER BY release_year;

-- GÃ©neros con mayor Ã­ndice de fiesta
SELECT track_genre, avg_party_index, track_count
FROM gold_genre_stats
ORDER BY avg_party_index DESC LIMIT 10;

-- Comparativa CSV vs API
SELECT source, COUNT(*) AS total, ROUND(AVG(popularity), 1) AS avg_popularity
FROM tracks GROUP BY source;

-- AuditorÃ­a: mensajes en Bronze que no pasaron a Gold
SELECT COUNT(*) AS en_bronze,
       (SELECT COUNT(*) FROM tracks) AS en_gold,
       COUNT(*) - (SELECT COUNT(*) FROM tracks) AS descartados
FROM bronze_raw;
```

## party_index

MÃ©trica propia del pipeline. No viene de Spotify.

```
party_index = danceability Ã— 0.40 + energy Ã— 0.35 + valence Ã— 0.25
```

Pesos justificados:
- **Danceability (40%)**: factor mÃ¡s determinante. Una canciÃ³n puede ser energÃ©tica pero no bailable.
- **Energy (35%)**: intensidad y actividad percibida.
- **Valence (25%)**: positividad musical. Una canciÃ³n triste puede ser bailable, pesa menos.

En PostgreSQL estÃ¡ definido como columna `GENERATED ALWAYS AS` en `audio_features`,
lo que garantiza que el cÃ¡lculo es siempre correcto aunque se inserte por otra vÃ­a.

## Tests

```bash
pip install pytest
pytest tests/ -v --tb=short
```

## Pre-commits y CI

```bash
pip install pre-commit
pre-commit install
# A partir de aquÃ­, Black, isort, Flake8, Bandit y Gitleaks
# se ejecutan automÃ¡ticamente en cada commit.
```

El workflow de GitHub Actions ejecuta lint â†’ tests â†’ build Docker en cada push.
