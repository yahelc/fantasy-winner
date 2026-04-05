FROM python:3.14-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Cache dir for scored data (parquet files, ephemeral per container)
RUN mkdir -p cache

EXPOSE 8080

CMD uvicorn web.app:app --host 0.0.0.0 --port ${PORT:-8080}
