#!/bin/bash
# Run a job with PID lock (prevents overlapping runs)
set -uo pipefail

# Source env vars written with proper quoting by entrypoint.sh
# shellcheck disable=SC1091
source /app/.env_runtime 2>/dev/null || source /etc/environment 2>/dev/null || true

JOB="$1"
TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
LOGFILE="/app/logs/${JOB}_${TIMESTAMP}.log"
PIDFILE="/tmp/${JOB}.pid"

echo "══════════════════════════════════════════════════"
echo "🚀 [$TIMESTAMP] Starting: ${JOB}"
echo "══════════════════════════════════════════════════"

# Prevent overlapping runs
if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    echo "⚠️  ${JOB} already running (PID $(cat $PIDFILE)) — skip"
    exit 0
fi

echo $$ > "$PIDFILE"

cd /app

case "$JOB" in
    scraper)
        python -u centris_scraper.py 2>&1 | tee "$LOGFILE"
        ;;
    cleaner)
        python -u cleaner.py 2>&1 | tee "$LOGFILE"
        ;;
    *)
        echo "❌ Unknown job: ${JOB}. Use: scraper | cleaner"
        exit 1
        ;;
esac

EXIT_CODE=$?
rm -f "$PIDFILE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ${JOB} done ($(date -u))"
else
    echo "❌ ${JOB} failed (exit ${EXIT_CODE})"
fi

# Rotate logs older than 30 days
find /app/logs -name "${JOB}_*.log" -mtime +30 -delete 2>/dev/null || true

# Chain: scraper → cleaner (no artificial delay between steps)
if [ "$JOB" = "scraper" ]; then
    echo ""
    echo "🧹 Scraper finished — triggering cleaner..."
    /app/runner.sh cleaner
fi

exit $EXIT_CODE
