#!/bin/bash
# ============================================================================
# MASTER BUILD SCRIPT: Agentic Silver Layer Demo
# ============================================================================
# Purpose: Execute all setup scripts in order for a clean build
# Prerequisites: 
#   - snow CLI installed and configured
#   - Connection named in $SF_CONNECTION (default: CoCo-Green)
#   - ACCOUNTADMIN role access
# ============================================================================

set -e

# Configuration
SF_CONNECTION="${SF_CONNECTION:-CoCo-Green}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "Agentic Silver Layer - Master Build Script"
echo "============================================"
echo "Connection: $SF_CONNECTION"
echo "Script Dir: $SCRIPT_DIR"
echo ""

# Function to run SQL script
run_sql() {
    local script=$1
    local name=$2
    echo "----------------------------------------"
    echo "Running: $name"
    echo "Script:  $script"
    echo "----------------------------------------"
    snow sql -c "$SF_CONNECTION" -f "$script"
    echo ""
}

# Function to run bash script
run_bash() {
    local script=$1
    local name=$2
    echo "----------------------------------------"
    echo "Running: $name"
    echo "Script:  $script"
    echo "----------------------------------------"
    bash "$script"
    echo ""
}

# ============================================================================
# PHASE 1: PostgreSQL Setup (Manual Steps)
# ============================================================================
echo "============================================"
echo "PHASE 1: PostgreSQL Setup"
echo "============================================"
echo ""
echo "NOTE: PostgreSQL instances require manual setup via Snowsight or CLI."
echo "Run these commands manually if instances don't exist:"
echo ""
echo "  # Create SOURCE instance"
echo "  snow sql -c $SF_CONNECTION -f $SCRIPT_DIR/01_source/setup.sql"
echo ""
echo "  # Create LANDING instance"
echo "  snow sql -c $SF_CONNECTION -f $SCRIPT_DIR/02_landing/setup.sql"
echo ""
echo "  # After instances are ready, seed data and setup replication"
echo "  # See 01_source/seed_data.sql and 03_replication/setup.sh"
echo ""
read -p "Press Enter to continue with Snowflake setup, or Ctrl+C to exit..."

# ============================================================================
# PHASE 2: Snowflake Database Setup
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 2: Snowflake Database & Schema Setup"
echo "============================================"
run_sql "$SCRIPT_DIR/04_openflow/setup.sql" "Database & Schema Creation"

# ============================================================================
# PHASE 3: Bronze Layer
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 3: Bronze Layer (VARIANT Dynamic Tables)"
echo "============================================"
run_sql "$SCRIPT_DIR/05_bronze/setup.sql" "Bronze VARIANT Dynamic Tables"

# ============================================================================
# PHASE 4: Silver Layer
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 4: Silver Layer (CDC-Aware Dynamic Tables)"
echo "============================================"
run_sql "$SCRIPT_DIR/06_silver/setup.sql" "Silver CDC Dynamic Tables"

# ============================================================================
# PHASE 5: Gold Layer
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 5: Gold Layer (Aggregation Dynamic Tables)"
echo "============================================"
run_sql "$SCRIPT_DIR/07_gold/setup.sql" "Gold Aggregation Tables"

# ============================================================================
# PHASE 6: Agents
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 6: Agentic Procedures (Cortex LLM)"
echo "============================================"
run_sql "$SCRIPT_DIR/08_agents/setup.sql" "Agent Procedures"

# ============================================================================
# PHASE 7: Semantic Views Pipeline
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 7: Semantic Views Pipeline"
echo "============================================"
run_sql "$SCRIPT_DIR/09_semantic_views/setup.sql" "Semantic View Pipeline Procedure"

echo ""
echo "Running the agentic semantic view pipeline..."
snow sql -c "$SF_CONNECTION" -q "CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE();"

# ============================================================================
# PHASE 8: Intelligence Setup
# ============================================================================
echo ""
echo "============================================"
echo "PHASE 8: Snowflake Intelligence Setup"
echo "============================================"
run_sql "$SCRIPT_DIR/10_intelligence/setup.sql" "Intelligence Permissions"

# ============================================================================
# VERIFICATION
# ============================================================================
echo ""
echo "============================================"
echo "VERIFICATION"
echo "============================================"
echo ""
echo "Checking pipeline status..."
snow sql -c "$SF_CONNECTION" -q "
SELECT 
    'LANDED' as stage, 
    (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.\"public\".customers) as row_count
UNION ALL 
SELECT 'BRONZE', (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT)
UNION ALL 
SELECT 'SILVER', (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS)
UNION ALL 
SELECT 'GOLD', (SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360)
UNION ALL 
SELECT 'SEMANTIC_VIEWS', (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS WHERE TABLE_SCHEMA = 'GOLD');
"

echo ""
echo "Checking semantic views..."
snow sql -c "$SF_CONNECTION" -q "SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD;"

echo ""
echo "Checking agent logs..."
snow sql -c "$SF_CONNECTION" -q "
SELECT status, COUNT(*) as cnt
FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG 
GROUP BY status;
"

echo ""
echo "============================================"
echo "BUILD COMPLETE!"
echo "============================================"
echo ""
echo "Next steps:"
echo "1. Configure Snowflake Intelligence in Snowsight"
echo "2. Add semantic views as data sources"
echo "3. Test with natural language queries"
echo ""
echo "For any failed semantic views, check:"
echo "  SELECT target_table, agent_reasoning"
echo "  FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG"
echo "  WHERE status = 'FAILED';"
