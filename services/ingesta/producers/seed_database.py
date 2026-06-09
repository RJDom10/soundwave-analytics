# services/ingesta/producers/seed_database.py
"""
Pobla MySQL con artistas y tracks reales desde MusicBrainz API.
Corre una sola vez para tener datos base en el proyecto.
"""

import time
import uuid
import logging
import requests
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Artistas de géneros variados para tener diversidad en los datos
SEED_ARTISTS = [
    "Bad Bunny", "Taylor Swift", "The Weeknd", "Drake", "Billie Eilish",
    "J Balvin", "Rosalía", "Kendrick Lamar", "Olivia Rodrigo", "Post Malone",
    "Dua Lipa", "Harry Styles", "Peso Pluma", "Karol G", "Shakira",
    "Ed Sheeran", "Ariana Grande", "BTS", "Coldplay", "Imagine Dragons",
]

MB_BASE = "https://musicbrainz.org/ws/2"
HEADERS  = {"User-Agent": "SoundWaveAnalytics/1.0 (portfolio-project)"}


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_HOST_PORT", "3307")),   # 3307 porque corre en WSL2
        database=os.getenv("MYSQL_DB", "soundwave_db"),
        user=os.getenv("MYSQL_USER", "sw_user"),
        password=os.getenv("MYSQL_PASSWORD"),
    )


def search_artist(name: str) -> dict | None:
    """Busca un artista en MusicBrainz y devuelve sus datos."""
    try:
        resp = requests.get(
            f"{MB_BASE}/artist",
            params={"query": name, "limit": 1, "fmt": "json"},
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("artists", [])
        if results:
            a = results[0]
            return {
                "artist_id":  a.get("id", str(uuid.uuid4())),
                "name":       a.get("name", name),
                "country":    a.get("country", "Unknown"),
                "genre":      (a.get("tags") or [{}])[0].get("name", "Pop"),
                "monthly_listeners": 0,
            }
    except Exception as e:
        logger.warning(f"Error buscando '{name}': {e}")
    return None


def get_tracks_for_artist(artist_id: str, artist_mb_id: str) -> list:
    """Obtiene hasta 5 tracks de un artista desde MusicBrainz."""
    try:
        resp = requests.get(
            f"{MB_BASE}/recording",
            params={
                "artist": artist_mb_id,
                "limit": 5,
                "fmt": "json",
            },
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        recordings = resp.json().get("recordings", [])
        tracks = []
        for r in recordings:
            tracks.append({
                "track_id":    r.get("id", str(uuid.uuid4())),
                "title":       r.get("title", "Unknown Track"),
                "artist_id":   artist_id,
                "duration_ms": r.get("length") or 210000,  # Default 3.5 min
                "explicit":    False,
                "popularity":  0,
            })
        return tracks
    except Exception as e:
        logger.warning(f"Error obteniendo tracks: {e}")
        return []


def insert_artist(cursor, artist: dict):
    cursor.execute("""
        INSERT IGNORE INTO artists (artist_id, name, country, genre, monthly_listeners)
        VALUES (%(artist_id)s, %(name)s, %(country)s, %(genre)s, %(monthly_listeners)s)
    """, artist)


def insert_track(cursor, track: dict):
    cursor.execute("""
        INSERT IGNORE INTO tracks (track_id, title, artist_id, duration_ms, explicit, popularity)
        VALUES (%(track_id)s, %(title)s, %(artist_id)s, %(duration_ms)s, %(explicit)s, %(popularity)s)
    """, track)


def run_seeds():
    conn = get_db_connection()
    cursor = conn.cursor()
    total_artists = 0
    total_tracks  = 0

    logger.info(f"🌱 Iniciando seeds con {len(SEED_ARTISTS)} artistas...")

    for name in SEED_ARTISTS:
        logger.info(f"  → Buscando: {name}")
        artist = search_artist(name)

        if not artist:
            logger.warning(f"  ✗ No encontrado: {name}")
            continue

        insert_artist(cursor, artist)
        conn.commit()
        total_artists += 1
        logger.info(f"  ✓ Artista guardado: {artist['name']} ({artist['country']})")

        # Obtener tracks del artista
        tracks = get_tracks_for_artist(artist["artist_id"], artist["artist_id"])
        for track in tracks:
            insert_track(cursor, track)
            total_tracks += 1

        conn.commit()

        # MusicBrainz pide máximo 1 req/seg
        time.sleep(1.1)

    cursor.close()
    conn.close()
    logger.info(f"\n✅ Seeds completados: {total_artists} artistas, {total_tracks} tracks")


if __name__ == "__main__":
    run_seeds()