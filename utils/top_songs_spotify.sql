-- DROP TABLE IF EXISTS public.top_songs_spotify;

CREATE TABLE IF NOT EXISTS public.top_songs_spotify
(
    danceability double precision,
    energy double precision,
    key bigint,
    loudness double precision,
    mode bigint,
    speechiness double precision,
    acousticness double precision,
    instrumentalness double precision,
    liveness double precision,
    valence double precision,
    tempo double precision,
    type text COLLATE pg_catalog."default",
    id text COLLATE pg_catalog."default",
    uri text COLLATE pg_catalog."default",
    track_href text COLLATE pg_catalog."default",
    analysis_url text COLLATE pg_catalog."default",
    duration_ms bigint,
    time_signature bigint,
    song text COLLATE pg_catalog."default",
    artist text COLLATE pg_catalog."default",
    explicit boolean,
    date timestamp without time zone,
    genre character varying(100) COLLATE pg_catalog."default"
)