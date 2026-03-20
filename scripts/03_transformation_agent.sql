-- =============================================================================
-- AGENTIC TRANSFORMATION AGENT: Cortex Agent for Silver Layer
-- Orchestrates discovery, analysis, and transformation execution
-- =============================================================================

USE DATABASE AGENTIC_PIPELINE;

-- Create Semantic View for the Agent to understand Bronze layer
CREATE OR REPLACE SEMANTIC VIEW AGENTS.BRONZE_METADATA_VIEW
AS SELECT * FROM METADATA.BRONZE_SCHEMA_REGISTRY
COMMENT = 'Registry of Bronze layer tables with schema analysis and transformation recommendations'
WITH SEMANTIC MODEL (
    tables:
      - name: BRONZE_SCHEMA_REGISTRY
        description: "Metadata about Bronze layer tables including detected columns, data quality issues, and recommended transformations"
        columns:
          - name: TABLE_NAME
            description: "Fully qualified name of the Bronze table"
            data_type: VARCHAR
          - name: DETECTED_COLUMNS
            description: "JSON object containing detected column names and inferred types"
            data_type: VARIANT
          - name: DATA_QUALITY_ISSUES
            description: "JSON array of identified data quality issues like nulls, type mismatches"
            data_type: VARIANT
          - name: RECOMMENDED_TRANSFORMATIONS
            description: "AI-generated transformation recommendations"
            data_type: VARIANT
);

-- Create the Transformation Agent
CREATE OR REPLACE AGENT AGENTS.SILVER_TRANSFORMATION_AGENT
  COMMENT = 'Agentic AI that discovers Bronze data and generates Silver layer transformations'
  PROFILE = '{"display_name": "Silver Transformation Agent", "avatar": "robot", "color": "#29B5E8"}'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are an expert data engineer responsible for transforming raw Bronze layer data into clean Silver layer tables. Your workflow: 1) Use discover_schema to understand Bronze table structure, 2) Use analyze_quality to identify data issues, 3) Use generate_transformation to create Dynamic Table SQL, 4) Log all transformations to the metadata registry.",
      "response": "Always explain your reasoning. Show the generated SQL. Recommend data quality fixes. Be concise but thorough.",
      "system": "You are the Silver Transformation Agent in an AI-First Data Architecture. Your goal is autonomous data transformation from Bronze to Silver layers using Snowflake best practices."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "generic",
          "name": "discover_schema",
          "description": "Discover the schema structure of a Bronze layer table by analyzing VARIANT payloads",
          "input_schema": {
            "type": "object",
            "properties": {
              "table_name": {
                "type": "string",
                "description": "Fully qualified table name (e.g., AGENTIC_PIPELINE.BRONZE.RAW_CUSTOMERS)"
              }
            },
            "required": ["table_name"]
          }
        }
      },
      {
        "tool_spec": {
          "type": "generic",
          "name": "analyze_quality",
          "description": "Analyze data quality issues in a Bronze table including nulls, type mismatches, and anomalies",
          "input_schema": {
            "type": "object",
            "properties": {
              "table_name": {
                "type": "string",
                "description": "Fully qualified table name to analyze"
              },
              "sample_size": {
                "type": "integer",
                "description": "Number of rows to sample for analysis (default 1000)"
              }
            },
            "required": ["table_name"]
          }
        }
      },
      {
        "tool_spec": {
          "type": "generic",
          "name": "generate_transformation",
          "description": "Generate a Dynamic Table transformation SQL from Bronze to Silver layer",
          "input_schema": {
            "type": "object",
            "properties": {
              "source_table": {
                "type": "string",
                "description": "Bronze layer source table"
              },
              "transformation_type": {
                "type": "string",
                "description": "Type: flatten_and_type, deduplicate, scd_type2, aggregate",
                "enum": ["flatten_and_type", "deduplicate", "scd_type2", "aggregate"]
              }
            },
            "required": ["source_table"]
          }
        }
      },
      {
        "tool_spec": {
          "type": "system_execute_sql",
          "name": "execute_transformation",
          "description": "Execute the generated transformation SQL to create Silver layer objects"
        }
      }
    ],
    "tool_resources": {
      "discover_schema": {
        "type": "procedure",
        "identifier": "AGENTIC_PIPELINE.AGENTS.DISCOVER_SCHEMA",
        "execution_environment": {
          "type": "warehouse",
          "name": "SNOWADHOC"
        }
      },
      "analyze_quality": {
        "type": "procedure",
        "identifier": "AGENTIC_PIPELINE.AGENTS.ANALYZE_DATA_QUALITY",
        "execution_environment": {
          "type": "warehouse",
          "name": "SNOWADHOC"
        }
      },
      "generate_transformation": {
        "type": "procedure",
        "identifier": "AGENTIC_PIPELINE.AGENTS.GENERATE_TRANSFORMATION",
        "execution_environment": {
          "type": "warehouse",
          "name": "SNOWADHOC"
        }
      },
      "execute_transformation": {
        "type": "sql",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "SNOWADHOC"
        }
      }
    }
  }
  $$;

-- Grant usage
GRANT USAGE ON AGENT AGENTS.SILVER_TRANSFORMATION_AGENT TO ROLE PUBLIC;
