FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements (if you have one, otherwise we install manually based on imports)
# For now, let's install the known packages
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    python-dotenv \
    google-cloud-storage \
    google-cloud-pubsub \
    google-cloud-aiplatform \
    asyncpg \
    scenedetect[opencv] \
    openai-whisper \
    replicate \
    pydantic \
    pydantic-settings \
    httpx \
    aiohttp \
    google-cloud-video-transcoder \
    google-cloud-videointelligence \
    tenacity

# Copy source code
COPY . .

# Set python path
ENV PYTHONPATH=/app/src

# Default command (overridden by docker-compose)
CMD ["python", "src/ingestion_worker/worker_pull.py"]
