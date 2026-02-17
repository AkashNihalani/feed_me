FROM python:3.11-slim

WORKDIR /worker

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
