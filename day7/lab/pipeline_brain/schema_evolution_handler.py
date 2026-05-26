from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    has_drift = bool(new_columns) or bool(removed_columns) or bool(type_changes)
    
    drift_severity = 'NONE'
    if new_columns and all('null' in v for v in new_columns.values()):
        drift_severity = 'LOW'
    elif new_columns or type_changes:
        drift_severity = 'HIGH'
    elif removed_columns:
        drift_severity = 'BREAKING'
    
    return {
        'new_columns': new_columns,
       'removed_columns': removed_columns,
        'type_changes': type_changes,
        'has_drift': has_drift,
        'drift_severity': drift_severity
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    decisions = {}
    for column, dtype in drift_report['new_columns'].items():
        if dtype =='string':
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 'LOW'}
        elif dtype == 'float':
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'New float column', 'risk_level': 'HIGH'}
        elif dtype == 'boolean':
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable boolean column', 'risk_level': 'LOW'}
    
    for column in drift_report['removed_columns']:
        decisions[column] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 'BREAKING'}
    
    for column, (old_type, new_type) in drift_report['type_changes'].items():
        if new_type == 'float' and old_type in ['int','string']:
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'Type widening', 'risk_level': 'LOW'}
        elif new_type in ['int','string'] and old_type == 'float':
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'Type narrowing', 'risk_level': 'HIGH'}
    
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for column, info in decisions.items():
        action = info['action']
        if action == 'DROP_SILENTLY':
            spark_df = spark_df.drop(column)
        elif action == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added column: {column} with type: {updated_schema[column]}")
        elif action == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{column}_anomaly", spark_df[column].isNull())
            migration_notes.append(f"Flagged anomaly for column: {column}")
        elif action == 'HALT':
            raise ValueError(f"Cannot silently drop column: {column}")
    
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    if not drift_report['has_drift']:
        print("No schema drift detected.")
        return drift_report
    
    decisions = decide_action(drift_report)
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        drift_report['migration_notes'] = migration_notes
        drift_report['evolved_df'] = evolved_df
    
    print("Schema drift detected:", drift_report['drift_severity'].capitalize())
    print("Drift details:", drift_report)
    return drift_report
