-- 01_schema.sql
-- Esquema inicial de la base de datos del pipeline IoT de Spotify.
-- Este script es ejecutado automáticamente por PostgreSQL la primera vez
-- que el contenedor arranca y el volumen está vacío.
--
-- Diseño normalizado:
--   artists   → artistas únicos
--   albums    → álbumes únicos, referencia a artists
--   tracks    → pistas, referencia a albums; campo source indica el origen del dato
--   audio_features → características de audio por pista; party_index es el campo
--                    calculado propio del ETL: (danceability + energy + valence) / 3

-- ---------------------------------------------------------------------------
-- Extensión para UUIDs (no requerida pero sí práctica)
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- Artistas
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artists (
    id          VARCHAR(22) PRIMARY KEY,  -- Spotify artist ID (22 chars base62)
    name        VARCHAR(255) NOT NULL,
    genres      TEXT[],                   -- Array de géneros asociados al artista
    popularity  SMALLINT,                 -- Popularidad 0-100 según Spotify
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Álbumes
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS albums (
    id            VARCHAR(22) PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    artist_id     VARCHAR(22) REFERENCES artists(id) ON DELETE CASCADE,
    release_date  DATE,
    total_tracks  SMALLINT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Pistas (tracks)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tracks (
    id           VARCHAR(22) PRIMARY KEY,  -- Spotify track ID
    name         VARCHAR(255) NOT NULL,
    artist_id    VARCHAR(22) REFERENCES artists(id) ON DELETE CASCADE,
    album_id     VARCHAR(22) REFERENCES albums(id) ON DELETE SET NULL,
    duration_ms  INTEGER,
    explicit     BOOLEAN DEFAULT FALSE,
    popularity   SMALLINT,
    -- 'csv' = dato proveniente del CSV histórico
    -- 'api' = dato obtenido en tiempo real desde la API de Spotify
    source       VARCHAR(10) NOT NULL CHECK (source IN ('csv', 'api')),
    ingested_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Índice para filtrar por fuente en los dashboards de Grafana
CREATE INDEX IF NOT EXISTS idx_tracks_source ON tracks(source);
-- Índice para filtrar por fecha de ingesta (queries de series temporales en Grafana)
CREATE INDEX IF NOT EXISTS idx_tracks_ingested_at ON tracks(ingested_at);

-- ---------------------------------------------------------------------------
-- Características de audio
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audio_features (
    track_id      VARCHAR(22) PRIMARY KEY REFERENCES tracks(id) ON DELETE CASCADE,
    danceability  NUMERIC(4,3),  -- 0.0 – 1.0
    energy        NUMERIC(4,3),  -- 0.0 – 1.0
    valence       NUMERIC(4,3),  -- 0.0 – 1.0 (positividad musical)
    tempo         NUMERIC(6,2),  -- BPM
    loudness      NUMERIC(6,2),  -- dB
    speechiness   NUMERIC(4,3),
    acousticness  NUMERIC(4,3),
    instrumentalness NUMERIC(4,3),
    liveness      NUMERIC(4,3),
    -- party_index: campo calculado por el ETL, NO viene de Spotify.
    -- Fórmula: (danceability + energy + valence) / 3
    -- Representa la "bailabilidad festiva" de la canción (0.0 – 1.0)
    party_index   NUMERIC(4,3)   GENERATED ALWAYS AS
                    ((danceability + energy + valence) / 3.0) STORED,
    key           SMALLINT,      -- Tono musical (0=C, 1=C#, ..., 11=B)
    mode          SMALLINT       CHECK (mode IN (0, 1)),  -- 0=menor, 1=mayor
    time_signature SMALLINT
);

-- Índice para los paneles de Grafana que ordenan por índice de fiesta
CREATE INDEX IF NOT EXISTS idx_audio_party_index ON audio_features(party_index DESC);

-- ---------------------------------------------------------------------------
-- Vista auxiliar: top de pistas con su índice de fiesta y origen
-- Grafana puede usar esta vista directamente sin SQL complejo en el panel
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_tracks_with_party AS
SELECT
    t.id,
    t.name          AS track_name,
    a.name          AS artist_name,
    al.name         AS album_name,
    t.source,
    t.ingested_at,
    af.danceability,
    af.energy,
    af.valence,
    af.party_index,
    af.tempo,
    af.loudness
FROM tracks t
JOIN artists a    ON t.artist_id = a.id
LEFT JOIN albums al   ON t.album_id  = al.id
LEFT JOIN audio_features af ON t.id = af.track_id;
