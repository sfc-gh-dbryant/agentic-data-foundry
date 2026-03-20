USE DATABASE DBAONTAP_ANALYTICS;

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
          "description": "Discover the schema structure of a Bronze layer table by analyzing VARIANT payloads using Cortex LLM",
          "input_schema": {
            "type": "object",
            "properties": {
              "table_name": {
                "type": "string",
                "description": "Fully qualified table name (e.g., DBAONTAP_ANALYTICS.BRONZE.RAW_CUSTOMERS)"
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
        "identifier": "DBAONTAP_ANALYTICS.AGENTS.CORTEX_INFER_SCHEMA",
        "execution_environment": {
          "type": "warehouse",
          "name": "SNOWADHOC"
        }
      },
      "analyze_quality": {
        "type": "procedure",
        "identifier": "DBAONTAP_ANALYTICS.AGENTS.ANALYZE_DATA_QUALITY",
        "execution_environment": {
          "type": "warehouse",
          "name": "SNOWADHOC"
        }
      },
      "generate_transformation": {
        "type": "procedure",
        "identifier": "DBAONTAP_ANALYTICS.AGENTS.GENERATE_TRANSFORMATION",
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

GRANT USAGE ON AGENT AGENTS.SILVER_TRANSFORMATION_AGENT TO ROLE PUBLIC;
