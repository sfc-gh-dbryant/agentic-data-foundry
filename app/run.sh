#!/bin/bash
# ============================================================================
# Run the Agentic Silver Layer Demo Manager Streamlit App
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Set environment variables
export SNOWFLAKE_CONNECTION_NAME="${SNOWFLAKE_CONNECTION_NAME:-CoCo-Green}"
export SOURCE_PG_HOST="${SOURCE_PG_HOST:-source-pg-host.example.snowflake.app}"
export LANDING_PG_HOST="${LANDING_PG_HOST:-landing-pg-host.example.snowflake.app}"

echo "Starting Agentic Silver Layer Demo Manager..."
echo "Snowflake Connection: $SNOWFLAKE_CONNECTION_NAME"
echo "SOURCE PG: $SOURCE_PG_HOST"
echo "LANDING PG: $LANDING_PG_HOST"
echo ""

# Check if dependencies are installed
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r "$SCRIPT_DIR/requirements.txt"
fi

# Run Streamlit
streamlit run "$SCRIPT_DIR/demo_manager.py" --server.port 8501 --server.headless true
