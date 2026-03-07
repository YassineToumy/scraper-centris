#!/bin/bash

echo "══════════════════════════════════════════════════"
echo "🇨🇦 Centris Service Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# Export env vars so cron can access them
printenv | grep -v "no_proxy" >> /etc/environment

# Write crontab — scraper runs daily (it also loops internally)
cat > /etc/cron.d/centris <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Scraper — every 24h at 03:00 UTC
0 3 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/centris

echo "✅ Cron schedule installed:"
echo "   03:00  🕷️  Scraper (daily)"
echo ""

# Verify MongoDB connection
echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGO_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo ""
echo "🔄 Running initial scrape on startup..."
/app/runner.sh scraper || echo "⚠️  Scraper startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
