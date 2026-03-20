#!/bin/bash
# ============================================================================
# 03_REPLICATION: Setup Logical Replication between SOURCE and LANDING
# ============================================================================
# Purpose: Configure PostgreSQL logical replication (one-time sync)
# Prerequisites: Both PostgreSQL instances running, credentials in ~/.pgpass
# ============================================================================

set -e

# Configuration - UPDATE THESE VALUES
SOURCE_HOST="${SOURCE_PG_HOST:-source-pg-host.example.snowflake.app}"
LANDING_HOST="${LANDING_PG_HOST:-landing-pg-host.example.snowflake.app}"
PG_USER="snowflake_admin"
PG_DB="dbaontap"

echo "============================================"
echo "Setting up PostgreSQL Logical Replication"
echo "============================================"
echo "SOURCE:  $SOURCE_HOST"
echo "LANDING: $LANDING_HOST"
echo ""

# Step 1: Verify SOURCE publication exists
echo "Step 1: Checking SOURCE publication..."
psql -h "$SOURCE_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT * FROM pg_publication;"

# Step 2: Create tables in LANDING if not exists
echo ""
echo "Step 2: Creating tables in LANDING..."
psql -h "$LANDING_HOST" -U "$PG_USER" -d "$PG_DB" -f "$(dirname $0)/../02_landing/create_tables.sql" 2>/dev/null || echo "Tables may already exist"

# Step 3: Get SOURCE password from .pgpass
SOURCE_PASS=$(grep "$SOURCE_HOST" ~/.pgpass | cut -d: -f5 | head -1)

if [ -z "$SOURCE_PASS" ]; then
    echo "ERROR: Cannot find SOURCE password in ~/.pgpass"
    echo "Add entry: $SOURCE_HOST:5432:*:$PG_USER:<password>"
    exit 1
fi

# Step 4: Create subscription in LANDING
echo ""
echo "Step 3: Creating subscription in LANDING..."
psql -h "$LANDING_HOST" -U "$PG_USER" -d "$PG_DB" << EOF
-- Drop existing subscription if any
DROP SUBSCRIPTION IF EXISTS dbaontap_sub;

-- Create new subscription
CREATE SUBSCRIPTION dbaontap_sub
    CONNECTION 'host=$SOURCE_HOST port=5432 dbname=$PG_DB user=$PG_USER password=$SOURCE_PASS'
    PUBLICATION dbaontap_pub
    WITH (copy_data = true);
EOF

# Step 5: Verify subscription status
echo ""
echo "Step 4: Verifying subscription..."
sleep 5
psql -h "$LANDING_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT subname, subenabled, subconninfo FROM pg_subscription;"
psql -h "$LANDING_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT * FROM pg_stat_subscription;"

# Step 6: Verify data sync
echo ""
echo "Step 5: Verifying data sync..."
echo "SOURCE counts:"
psql -h "$SOURCE_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT 'customers' as tbl, COUNT(*) FROM customers UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'products', COUNT(*) FROM products;"

echo ""
echo "LANDING counts:"
psql -h "$LANDING_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT 'customers' as tbl, COUNT(*) FROM customers UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'products', COUNT(*) FROM products;"

echo ""
echo "============================================"
echo "Logical replication setup complete!"
echo "============================================"
