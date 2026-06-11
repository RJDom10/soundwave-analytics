# services/ingesta/producers/event_simulator.py
"""
Simula eventos de reproducción de usuarios en tiempo real.
Publica en el topic raw-play-events de Kafka.

Distribución realista:
  65% → play completo  (> 30 segundos)
  20% → skip temprano  (< 5 segundos)
  10% → play parcial   (5s – 30s)
   5% → like
"""

import json
import random
import time
import uuid
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Catálogos de valores posibles ────────────────────────────────────
DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web"]
SOURCES      = ["radio", "search", "playlist", "recommendation"]
COUNTRIES    = ["MX", "CO", "AR", "CL", "PE", "US", "ES", "BR"]

# Coordenadas aproximadas por país (para simular geolocalización)
GEO_BY_COUNTRY = {
    "MX": (-99.1332, 19.4326),
    "CO": (-74.0721, 4.7110),
    "AR": (-58.3816, -34.6037),
    "CL": (-70.6693, -33.4489),
    "PE": (-77.0428, -12.0464),
    "US": (-87.6298, 41.8781),
    "ES": (-3.7038,   40.4168),
    "BR": (-43.1729, -22.9068),
}


def get_db_connection():
    """Conexión a MySQL desde WSL2 (host, puerto 3307)."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_HOST_PORT", "3307")),
        database=os.getenv("MYSQL_DB", "soundwave_db"),
        user=os.getenv("MYSQL_USER", "sw_user"),
        password=os.getenv("MYSQL_PASSWORD"),
    )


def load_track_ids() -> list:
    """Carga los track_ids reales desde MySQL."""
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT track_id FROM tracks")
        ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        if ids:
            logger.info(f"✅ Cargados {len(ids)} tracks desde MySQL")
            return ids
        else:
            logger.warning("⚠️  No hay tracks en MySQL, usando IDs simulados")
    except Exception as e:
        logger.warning(f"⚠️  Sin conexión a MySQL ({e}), usando IDs simulados")

    # Fallback: IDs simulados si MySQL no está disponible
    return [str(uuid.uuid4()) for _ in range(200)]


class EventSimulator:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            compression_type="gzip",
        )
        self.topic    = "raw-play-events"
        self.track_ids = load_track_ids()

        # Pool de usuarios simulados (persistente durante la ejecución)
        self.user_ids = [str(uuid.uuid4()) for _ in range(500)]
        logger.info(f"🎵 Simulador listo: {len(self.track_ids)} tracks | {len(self.user_ids)} usuarios simulados")

    def _generate_event(self) -> dict:
        """Genera un evento con distribución realista."""
        rand = random.random()

        if rand < 0.65:
            event_type = "play_complete"
            duration   = random.randint(30_000, 300_000)   # 30s – 5min
        elif rand < 0.85:
            event_type = "skip"
            duration   = random.randint(500, 5_000)        # 0.5s – 5s
        elif rand < 0.95:
            event_type = "play_partial"
            duration   = random.randint(5_000, 29_999)     # 5s – 30s
        else:
            event_type = "like"
            duration   = random.randint(10_000, 300_000)

        country = random.choice(COUNTRIES)
        lon, lat = GEO_BY_COUNTRY.get(country, (0.0, 0.0))

        return {
            "event_id":          str(uuid.uuid4()),
            "event_type":        event_type,
            "user_id":           random.choice(self.user_ids),
            "track_id":          random.choice(self.track_ids),
            "played_at":         datetime.now(timezone.utc).isoformat(),
            "duration_played_ms": duration,
            "source":            random.choice(SOURCES),
            "device_type":       random.choice(DEVICE_TYPES),
            "country":           country,
            "latitude":          round(lat + random.uniform(-2, 2), 4),
            "longitude":         round(lon + random.uniform(-2, 2), 4),
        }

    def run(self, events_per_second: int = 5):
        """
        Genera y publica eventos continuamente.
        5 eventos/seg es suficiente para ver el pipeline en acción
        sin saturar tu laptop.
        """
        sleep_time  = 1.0 / events_per_second
        total       = 0

        logger.info(f"🚀 Simulador iniciado: {events_per_second} eventos/seg")
        logger.info("   Presiona Ctrl+C para detener\n")

        while True:
            try:
                event = self._generate_event()
                self.producer.send(
                    self.topic,
                    key=event["user_id"],
                    value=event,
                )
                total += 1

                # Log cada 100 eventos para no saturar la consola
                if total % 100 == 0:
                    self.producer.flush()
                    logger.info(
                        f"📊 Eventos publicados: {total:,} | "
                        f"Último: {event['event_type']} | "
                        f"País: {event['country']}"
                    )

                time.sleep(sleep_time)

            except KeyboardInterrupt:
                self.producer.flush()
                logger.info(f"\n🛑 Simulador detenido. Total eventos: {total:,}")
                break
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                time.sleep(5)

        self.producer.close()


if __name__ == "__main__":
    simulator = EventSimulator()
    simulator.run(events_per_second=5)