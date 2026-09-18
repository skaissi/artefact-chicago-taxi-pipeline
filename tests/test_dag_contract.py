"""Static import check: no Docker, Airflow runtime, network or Spark needed."""
import ast
from pathlib import Path


DAG_FILE = Path(__file__).resolve().parents[1] / 'dags' / 'chicago_taxi.py'


def test_dag_has_five_tasks_in_expected_order():
    tree = ast.parse(DAG_FILE.read_text(encoding='utf-8'))
    functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    names = [node.name for node in functions]
    assert names == ['_settings_for_run', 'chicago_taxi_pipeline', 'bronze_ingestion', 'silver_processing',
                     'gold_candidate', 'quality_gate', 'publish_gold']
    dag_function = next(fn for fn in functions if fn.name == 'chicago_taxi_pipeline')
    assert len(dag_function.decorator_list) == 1
    assert 'schedule=None' in DAG_FILE.read_text(encoding='utf-8')
    assert 'max_active_runs=1' in DAG_FILE.read_text(encoding='utf-8')


def test_dag_no_heavy_imports_during_parse():
    tree = ast.parse(DAG_FILE.read_text(encoding='utf-8'))
    top_level_imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    imported = [alias.name for node in top_level_imports for alias in node.names]
    assert not any(name.startswith(('pyspark', 'taxi_pipeline', 'boto3')) for name in imported)


def test_publication_requires_gate_dependency():
    text = DAG_FILE.read_text(encoding="utf-8")
    assert "gate = quality_gate(bronze, silver, candidate)" in text
    assert "publish_gold(candidate, gate)" in text
