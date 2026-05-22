# Pipeline IoT de Spotify

Pipeline de datos IoT que ingesta canciones desde un CSV histórico y la API de Spotify, las transmite mediante MQTT (Mosquitto), las persiste en PostgreSQL y las visualiza en Grafana. El ETL está orquestado con Apache Airflow.

---

## Arquitectura

```
CSV / API Spotify
      │
      ▼
 [Productor MQTT]
      │  topic: spotify/tracks
      ▼
  [Mosquitto]          ← broker MQTT (protocolo estándar IoT)
      │
      ▼
 [Consumidor MQTT]
      │  INSERT / ON CONFLICT DO NOTHING
      ▼
  [PostgreSQL]         ← almacén relacional con esquema normalizado
      │
      ├──── [Grafana]  ← dashboards auto-provisionados
      │
      └──── [Airflow]  ← DAG ETL horario (extract → transform → load)
```

**Flujo de datos:**
1. El **productor** lee el CSV fila a fila (cada 5 s) y publica en MQTT con `source=csv`.
2. Cada hora llama a `/browse/new-releases` de Spotify y publica con `source=api`.
3. El **consumidor** está suscrito al topic MQTT y persiste cada mensaje en PostgreSQL.
4. El DAG de **Airflow** se ejecuta cada hora: extrae de Spotify, calcula `party_index` y carga en PostgreSQL.
5. **Grafana** lee directamente de PostgreSQL y refresca los dashboards cada 30 s.

---

## Requisitos previos

- Docker Desktop ≥ 24
- Python 3.11 (solo para `make fernet` y pre-commit)
- Cuenta de desarrollador de Spotify → [developer.spotify.com](https://developer.spotify.com/dashboard)

---

## Puesta en marcha

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd Final
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` y rellenar:

| Variable | Descripción |
|---|---|
| `POSTGRES_PASSWORD` | Contraseña de PostgreSQL |
| `GRAFANA_PASSWORD` | Contraseña del admin de Grafana |
| `AIRFLOW_FERNET_KEY` | Clave Fernet de Airflow (ver abajo) |
| `AIRFLOW_ADMIN_PASSWORD` | Contraseña del admin de Airflow |
| `SPOTIFY_CLIENT_ID` | Client ID de la app en Spotify Developer |
| `SPOTIFY_CLIENT_SECRET` | Client Secret de la app en Spotify Developer |

**Generar la clave Fernet:**

```bash
make fernet
# Copia la salida en AIRFLOW_FERNET_KEY del fichero .env
```

### 3. Arrancar el stack completo

```bash
make up
```

| Servicio | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Airflow | http://localhost:8080 |
| PostgreSQL | localhost:5432 |
| Mosquitto MQTT | localhost:1883 |

### 4. Verificar que los datos llegan

```bash
# Ver logs del consumidor en tiempo real
docker compose logs -f consumer

# Contar pistas en PostgreSQL
docker exec -it postgres psql -U spotify_user -d spotify_db \
  -c "SELECT source, COUNT(*) FROM tracks GROUP BY source;"
```

---

## Esquema de la base de datos

```
artists
  id, name, genres[], popularity

albums
  id, name, artist_id → artists, release_date, total_tracks

tracks
  id, name, artist_id → artists, album_id → albums,
  duration_ms, explicit, popularity,
  source ('csv' | 'api'),   ← origen del dato
  ingested_at

audio_features
  track_id → tracks,
  danceability, energy, valence,
  party_index GENERATED AS (danceability + energy + valence) / 3,  ← campo propio
  tempo, loudness, speechiness, acousticness, instrumentalness,
  liveness, key, mode, time_signature
```

### Índice de Fiesta (`party_index`)

Campo calculado por el pipeline (no viene de Spotify):

```
party_index = (danceability + energy + valence) / 3
```

Representa la "bailabilidad festiva" de una canción (0.0 – 1.0). Se calcula tanto en la capa de transformación de Airflow como en la definición de la tabla PostgreSQL (`GENERATED ALWAYS AS ... STORED`).

---

## Dashboards de Grafana

Los dashboards se provisionan automáticamente al arrancar el contenedor. No requieren configuración manual.

**Paneles incluidos:**
- Total de pistas ingestadas
- Índice de Fiesta medio (con umbral de color verde/amarillo/rojo)
- Distribución de pistas por fuente (CSV vs API) — gráfico donut
- Pistas ingestadas por minuto — serie temporal
- Top 10 canciones por índice de fiesta — barra horizontal
- Energía vs bailabilidad por fuente

---

## DAG de Airflow

El DAG `spotify_etl` tiene tres tareas encadenadas:

```
extract_from_api → transform → load_to_postgres
```

- **extract_from_api**: llama a `/browse/new-releases` y `/search?genre:pop`
- **transform**: normaliza campos, calcula `party_index`
- **load_to_postgres**: inserta con `ON CONFLICT DO NOTHING` (idempotente)

Acceder a la UI: http://localhost:8080 (usuario/contraseña definidos en `.env`)

---

## Calidad de código y CI/CD

### Pre-commit (local)

```bash
make install-hooks   # instala los hooks en el repo
make lint            # ejecuta todos los hooks manualmente
```

**Hooks configurados:**

| Hook | Propósito |
|---|---|
| `black` | Formato de código Python |
| `isort` | Orden de imports |
| `flake8` | Linter PEP 8 |
| `bandit` | Análisis de seguridad del código (inyecciones SQL, funciones peligrosas) |
| `gitleaks` | Detección de credenciales en el historial de Git |
| `check-yaml` / `check-json` | Validación de ficheros de configuración |

### GitHub Actions (CI/CD)

Pipeline en `.github/workflows/ci.yml`:

1. **lint** — pre-commit sobre todo el código
2. **test** — pytest con PostgreSQL de servicio (cobertura mínima 70%)
3. **build** — construcción de las 3 imágenes Docker

Se activa en cada push y pull request a `main` y `develop`.

---

## Tests

```bash
make test
# o directamente:
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Parar el stack

```bash
make down        # para los contenedores (datos persistidos en volúmenes)
make down-v      # para los contenedores Y borra los volúmenes (datos eliminados)
```
