# Productores de Ingesta

## Setup del entorno

```bash
python3 -m venv soundwave
source soundwave/bin/activate
pip install -r requirements.txt
```

## Notas importantes

- Usar `kafka-python-ng` en lugar de `kafka-python` (obsoleta y con errores de compatibilidad)
- Conexiones desde WSL2 al host usan `MYSQL_HOST=127.0.0.1` y puerto `3307`
- Conexiones desde dentro de Docker usan `MYSQL_HOST=mysql` y puerto `3306`

## Ejecutar el productor de Last.fm

```bash
source soundwave/bin/activate
python lastfm_producer.py
```

## Ejecutar el simulador de eventos

```bash
source soundwave/bin/activate
python event_simulator.py
```