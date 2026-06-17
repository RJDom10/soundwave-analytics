# services/ingesta/producers/lastfm_producer.py
"""
Jala tendencias de Last.fm y las publica en el topic
raw-track-metadata de Kafka cada 5 minutos.
"""

import json
import time
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class LastFmProducer:

    BASE_URL = "http://ws.audioscrobbler.com/2.0/"

    def __init__(self):
        self.api_key = os.getenv("LASTFM_API_KEY")
        if not self.api_key:
            raise ValueError("❌ LASTFM_API_KEY no está en tu .env")

        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            compression_type="gzip",
        )
        self.topic = "raw-track-metadata"

    def fetch_top_tracks(self, country: str = "mexico", limit: int = 50) -> list:
        """Top tracks de un país."""
        try:
            resp = requests.get(
                self.BASE_URL,
                params={
                    "method":  "geo.gettoptracks",
                    "country": country,
                    "api_key": self.api_key,
                    "format":  "json",
                    "limit":   limit,
                },
                timeout=10,
            )
            resp.raise_for_status()
            tracks = resp.json().get("tracks", {}).get("track", [])
            logger.info(f"📡 Last.fm ({country}): {len(tracks)} tracks obtenidos")
            return tracks
        except Exception as e:
            logger.error(f"Error Last.fm API: {e}")
            return []

    def fetch_global_chart(self, limit: int = 100) -> list:
        """Chart global de Last.fm."""
        try:
            resp = requests.get(
                self.BASE_URL,
                params={
                    "method":  "chart.gettoptracks",
                    "api_key": self.api_key,
                    "format":  "json",
                    "limit":   limit,
                },
                timeout=10,
            )
            resp.raise_for_status()
            tracks = resp.json().get("tracks", {}).get("track", [])
            logger.info(f"📡 Last.fm (global): {len(tracks)} tracks obtenidos")
            return tracks
        except Exception as e:
            logger.error(f"Error Last.fm chart: {e}")
            return []

    def publish(self, track: dict, source: str) -> bool:
        """Publica un track en Kafka."""
        artist = track.get("artist", {})
        message = {
            "track_id":    track.get("mbid") or f"lfm_{track.get('name','').replace(' ','_')[:40]}",
            "title":       track.get("name"),
            "artist":      artist.get("name") if isinstance(artist, dict) else artist,
            "play_count":  int(track.get("playcount", 0)),
            "listeners":   int(track.get("listeners", 0)),
            "source":      source,
            "url":         track.get("url"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.producer.send(self.topic, key=message["track_id"], value=message)
            return True
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            return False

    def run(self, interval: int = 300):
        """Loop principal: corre cada `interval` segundos."""
        logger.info(f"🎵 LastFmProducer iniciado. Intervalo: {interval}s")
        while True:
            try:
                count = 0
                for track in self.fetch_global_chart(100):
                    if self.publish(track, "lastfm_global"):
                        count += 1
                for track in self.fetch_top_tracks("mexico", 50):
                    if self.publish(track, "lastfm_mexico"):
                        count += 1
                for track in self.fetch_top_tracks("colombia", 50):
                    if self.publish(track, "lastfm_colombia"):
                        count += 1

                self.producer.flush()
                logger.info(f"✅ {count} tracks publicados en Kafka. Próximo ciclo en {interval}s...")
                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("🛑 Productor detenido")
                break
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                time.sleep(30)

        self.producer.close()


if __name__ == "__main__":
    producer = LastFmProducer()
    producer.run(interval=300)