FROM python:3.11-slim

WORKDIR /worker

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg \
  && rm -rf /var/lib/apt/lists/*

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker code
COPY . .

# Health check: verify Python and the module can import
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "from app.pure_engine import PureEngine; print('ok')" || exit 1

# Run the worker (infinite loop, auto-restarts via docker)
CMD ["python", "-m", "app.cli", "--mode", "worker"]
