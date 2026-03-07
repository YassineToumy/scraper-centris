FROM python:3.12-slim

WORKDIR /app

# Install cron + curl + system deps for Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium with all system dependencies
RUN playwright install --with-deps chromium

# Copy scripts
COPY centris_scraper.py ./
COPY entrypoint.sh /entrypoint.sh
COPY runner.sh ./

RUN chmod +x runner.sh /entrypoint.sh
RUN mkdir -p /app/logs

ENTRYPOINT ["/entrypoint.sh"]
