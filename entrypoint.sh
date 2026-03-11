#!/bin/bash

echo "══════════════════════════════════════════════════"
echo "🇨🇦 Centris Service Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# Export env vars with proper shell quoting so cron and runner.sh can source them.
# printenv >> /etc/environment is NOT safe — URIs with @, ?, &, + break when sourced.
env | while IFS='=' read -r k v; do
    [[ "$k" == "no_proxy" || "$k" == "_" ]] && continue
    printf 'export %s=%q\n' "$k" "$v"
done > /app/.env_runtime
# Also append to /etc/environment for cron (simple format, cron reads it directly)
printenv | grep -v -E "^(no_proxy|_)=" >> /etc/environment

# Write crontab
cat > /etc/cron.d/centris <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Scraper — every 24h at 03:00 UTC; cleaner + sync chain automatically after
0 3 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/centris

echo "✅ Cron schedule installed:"
echo "   03:00  🕷️  Scraper → 🧹 Cleaner → 🔄 Sync (chained, daily)"
echo ""

# Verify connections
echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGODB_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo "🔌 Testing PostgreSQL..."
python -c "
import psycopg2, os
conn = psycopg2.connect(os.environ['POSTGRES_DSN'])
cur = conn.cursor()
cur.execute('SELECT 1')
print('  ✅ PostgreSQL OK')
conn.close()
" || echo "  ❌ PostgreSQL connection failed"

echo ""
echo "🔄 Running initial pipeline on startup (scraper → cleaner → sync)..."

# scraper chains cleaner, cleaner chains sync — one call runs all three
/app/runner.sh scraper || echo "⚠️  Pipeline startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
