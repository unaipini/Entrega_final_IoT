-- 01_schema.sql
-- Esquema de la base de datos del pipeline IoT de Spotify.
-- Arquitectura Medallón completa: Bronze (raw) + Gold (analytics)
-- Ejecutado automáticamente por PostgreSQL al arrancar por primera vez.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ===========================================================================
-- CAPA BRONZE — Datos crudos
-- Cada mensaje MQTT recibido se guarda aquí sin tocar.
-- Permite auditoría completa: qué llegó al sistema vs qué pasó validación.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS bronze_raw (
    id          SERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload     JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_payload ON bronze_raw USING GIN (payload);

-- ===========================================================================
-- CAPA GOLD — Esquema normalizado de producción
-- ===========================================================================

CREATE TABLE IF NOT EXISTS artists (
    id          VARCHAR(22)  PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    genres      TEXT[],
    popularity  SMALLINT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS albums (
    id            VARCHAR(22)  PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    artist_id     VARCHAR(22)  REFERENCES artists(id) ON DELETE CASCADE,
    release_date  DATE,
    release_year  SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM release_date)::SMALLINT) STORED,
    total_tracks  SMALLINT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_albums_release_year ON albums(release_year);

CREATE TABLE IF NOT EXISTS tracks (
    id           VARCHAR(22)  PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    artist_id    VARCHAR(22)  REFERENCES artists(id) ON DELETE CASCADE,
    album_id     VARCHAR(22)  REFERENCES albums(id)  ON DELETE SET NULL,
    duration_ms  INTEGER,
    duration_min NUMERIC(5,2) GENERATED ALWAYS AS (ROUND(duration_ms / 60000.0, 2)) STORED,
    explicit     BOOLEAN      DEFAULT FALSE,
    popularity   SMALLINT,
    source       VARCHAR(10)  NOT NULL CHECK (source IN ('csv', 'api')),
    ingested_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracks_source      ON tracks(source);
CREATE INDEX IF NOT EXISTS idx_tracks_ingested_at ON tracks(ingested_at);
CREATE INDEX IF NOT EXISTS idx_tracks_popularity  ON tracks(popularity DESC);

CREATE TABLE IF NOT EXISTS audio_features (
    track_id          VARCHAR(22) PRIMARY KEY REFERENCES tracks(id) ON DELETE CASCADE,
    danceability      NUMERIC(4,3),
    energy            NUMERIC(4,3),
    valence           NUMERIC(4,3),
    tempo             NUMERIC(6,2),
    loudness          NUMERIC(6,2),
    speechiness       NUMERIC(4,3),
    acousticness      NUMERIC(4,3),
    instrumentalness  NUMERIC(4,3),
    liveness          NUMERIC(4,3),
    key               SMALLINT,
    mode              SMALLINT CHECK (mode IN (0, 1)),
    time_signature    SMALLINT,
    -- party_index ponderado: danceability(40%) > energy(35%) > valence(25%)
    -- Una canción puede ser energética pero no bailable, de ahí el mayor peso de danceability.
    party_index       NUMERIC(4,3) GENERATED ALWAYS AS (
                          ROUND(danceability * 0.40 + energy * 0.35 + valence * 0.25, 3)
                      ) STORED
);

CREATE INDEX IF NOT EXISTS idx_audio_party_index ON audio_features(party_index DESC);

-- Agregado pre-calculado por género (se refresca cada 500 mensajes)
CREATE TABLE IF NOT EXISTS gold_genre_stats (
    track_genre         TEXT        PRIMARY KEY,
    track_count         INTEGER,
    avg_popularity      NUMERIC(5,2),
    avg_danceability    NUMERIC(4,3),
    avg_energy          NUMERIC(4,3),
    avg_valence         NUMERIC(4,3),
    avg_tempo           NUMERIC(6,2),
    avg_party_index     NUMERIC(4,3),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tendencias temporales por año (se refresca cada 500 mensajes)
CREATE TABLE IF NOT EXISTS gold_temporal_trends (
    release_year        SMALLINT    PRIMARY KEY,
    track_count         INTEGER,
    avg_danceability    NUMERIC(4,3),
    avg_energy          NUMERIC(4,3),
    avg_valence         NUMERIC(4,3),
    avg_acousticness    NUMERIC(4,3),
    avg_party_index     NUMERIC(4,3),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Vistas para Grafana
CREATE OR REPLACE VIEW vw_tracks_with_party AS
SELECT
    t.id,
    t.name              AS track_name,
    a.name              AS artist_name,
    al.name             AS album_name,
    al.release_year,
    t.source,
    t.ingested_at,
    af.danceability,
    af.energy,
    af.valence,
    af.party_index,
    af.tempo,
    af.loudness,
    af.speechiness,
    af.acousticness,
    t.popularity,
    t.explicit,
    t.duration_min
FROM tracks t
JOIN  artists       a  ON t.artist_id = a.id
LEFT JOIN albums    al ON t.album_id  = al.id
LEFT JOIN audio_features af ON t.id  = af.track_id;

CREATE OR REPLACE VIEW vw_top_party_tracks AS
SELECT track_name, artist_name, album_name, party_index,
       danceability, energy, valence, popularity, source
FROM vw_tracks_with_party
WHERE party_index IS NOT NULL
ORDER BY party_index DESC
LIMIT 100;
