FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg and cron
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev cron curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

# Cron config will be mounted or copied
COPY app/cron/scheduler.cron /etc/cron.d/scheduler
RUN chmod 0644 /etc/cron.d/scheduler \
    && crontab /etc/cron.d/scheduler

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "app.cli", "--mode", "worker"]
