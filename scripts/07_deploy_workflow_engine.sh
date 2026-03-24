#!/bin/bash
# =============================================================================
# 07_deploy_workflow_engine.sh
# Deploys the modular agentic workflow engine (07a through 07h) in order.
# =============================================================================
set -e

SF_CONNECTION="${SF_CONNECTION:-CoCo-Green}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "Deploying Agentic Workflow Engine (Modular)"
echo "============================================"
echo "Connection: $SF_CONNECTION"
echo ""

run_sql() {
    local script=$1
    local name=$2
    echo "  [$name] deploying..."
    /opt/homebrew/Cellar/snowflake-cli/3.13.0/libexec/bin/snow sql -c "$SF_CONNECTION" -f "$script" 2>&1
    if [ $? -eq 0 ]; then
        echo "  [$name] OK"
    else
        echo "  [$name] FAILED"
        exit 1
    fi
    echo ""
}

SCRIPTS=(
    "07a_metadata_tables.sql:Metadata Tables"
    "07b_trigger.sql:Trigger Phase"
    "07c_planner.sql:Planner Phase"
    "07d_executor.sql:Executor Phase"
    "07e_validator.sql:Validator Phase"
    "07f_reflector.sql:Reflector Phase"
    "07g_orchestrator.sql:Orchestrator"
    "07h_monitoring.sql:Monitoring & Task"
)

for entry in "${SCRIPTS[@]}"; do
    IFS=':' read -r file label <<< "$entry"
    run_sql "$SCRIPT_DIR/$file" "$label"
done

echo "============================================"
echo "Workflow Engine Deployed Successfully!"
echo "============================================"
echo ""
echo "Test with:  CALL DBAONTAP_ANALYTICS.AGENTS.RUN_AGENTIC_WORKFLOW('manual');"
echo "Monitor:    SELECT * FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_DASHBOARD;"
