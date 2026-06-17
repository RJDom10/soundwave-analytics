FROM python:3.11-slim

WORKDIR /app

# Optimizamos la caché de Docker instalando dependencias primero
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el código de los productores
COPY . .

# Dejamos el ENTRYPOINT listo para ejecutar Python
ENTRYPOINT ["python"]