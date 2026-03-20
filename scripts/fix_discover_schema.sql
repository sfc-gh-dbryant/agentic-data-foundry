CREATE OR REPLACE PROCEDURE DBAONTAP_ANALYTICS.AGENTS.DISCOVER_SCHEMA(TABLE_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'discover'
EXECUTE AS OWNER
AS
$$
import json

def discover(session, table_name):
    cols = session.sql(f'DESC TABLE {table_name}').collect()
    col_names = [c['name'] for c in cols]
    col_types = {c['name']: c['type'] for c in cols}

    variant_cols = [c for c in col_names if col_types.get(c, '').startswith(('VARIANT', 'OBJECT'))]

    count = session.sql(f'SELECT COUNT(*) as cnt FROM {table_name}').collect()[0]['CNT']

    result = {
        'table_name': table_name,
        'row_count': count,
        'columns': col_names,
        'column_types': col_types,
        'discovered_columns': {}
    }

    if variant_cols:
        payload_col = variant_cols[0]
        try:
            keys_df = session.sql(f"""
                SELECT DISTINCT f.key, 
                       TYPEOF(f.value) as val_type,
                       MAX(f.value::VARCHAR) as sample_val
                FROM (SELECT * FROM {table_name} LIMIT 100) src,
                     LATERAL FLATTEN(input => src.{payload_col}) f
                GROUP BY f.key, TYPEOF(f.value)
                ORDER BY f.key
            """).collect()

            schema = {}
            type_map = {
                'INTEGER': 'int', 'DECIMAL': 'float', 'DOUBLE': 'float',
                'VARCHAR': 'str', 'BOOLEAN': 'bool', 'NULL_VALUE': 'null',
                'ARRAY': 'list', 'OBJECT': 'dict', 'TIMESTAMP_NTZ': 'str',
                'TIMESTAMP_LTZ': 'str', 'DATE': 'str'
            }
            for row in keys_df:
                key = row['KEY']
                raw_type = row['VAL_TYPE']
                sample = row['SAMPLE_VAL']
                schema[key] = {
                    'inferred_type': type_map.get(raw_type, raw_type),
                    'sample_value': str(sample)[:50] if sample else None
                }
            result['discovered_columns'] = schema
            result['variant_column'] = payload_col
        except Exception as e:
            result['variant_analysis_error'] = str(e)

    result['column_count'] = len(result.get('discovered_columns', {})) or len(col_names)
    return result
$$;
