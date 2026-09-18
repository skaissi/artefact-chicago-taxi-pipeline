"""Regression tests for V2.1: no staging prefix and consistent batch-specific candidates.

Pure AST/Python checks: no Spark, Docker or S3 required.
"""
import ast
from dataclasses import replace
from pathlib import Path

import pytest

from taxi_pipeline.config import Settings


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "dags" / "chicago_taxi.py"
PIPELINE_PATH = ROOT / "src" / "taxi_pipeline" / "pipeline.py"


def _function_from_source(source: Path, name: str, namespace: dict):
    tree = ast.parse(source.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    code = compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec")
    exec(code, namespace)
    return namespace[name]


def test_airflow_extraction_params_are_dates_with_env_defaults():
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    dag_func = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "chicago_taxi_pipeline")
    dag_decorator = dag_func.decorator_list[0]
    params = next(k.value for k in dag_decorator.keywords if k.arg == "params")
    names = [k.value for k in params.keys]
    assert names == ["extract_start_date", "extract_end_date"]
    for value in params.values:
        attributes = {k.arg: k.value.value for k in value.keywords if isinstance(k.value, ast.Constant)}
        assert attributes["type"] == "string"
        assert attributes["format"] == "date"
        assert isinstance(value.args[0], ast.Call)
        assert isinstance(value.args[0].func, ast.Attribute)
        assert value.args[0].func.attr == "getenv"


def test_all_five_tasks_resolve_the_running_dag_params():
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    names = {"bronze_ingestion", "silver_processing", "gold_candidate", "quality_gate", "publish_gold"}
    task_functions = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in task_functions} == names
    for task in task_functions:
        settings_calls = [n for n in ast.walk(task) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                          and n.func.id == "_settings_for_run"]
        assert len(settings_calls) == 1, f"{task.name} must use per-run dates exactly once"


def test_runtime_settings_override_env_and_preserve_other_settings(monkeypatch):
    monkeypatch.setenv("START_DATE", "2023-01-01")
    monkeypatch.setenv("END_DATE", "2023-01-08")
    monkeypatch.setenv("MAX_ROWS", "42")
    params = {"extract_start_date": "2023-02-01", "extract_end_date": "2023-03-01"}
    fn = _function_from_source(DAG_PATH, "_settings_for_run", {
        "replace": replace, "get_current_context": lambda: {"params": params}
    })
    settings = fn()
    assert (settings.start_date, settings.end_date) == ("2023-02-01", "2023-03-01")
    assert settings.max_rows == 42
    assert settings.dataset_prefix == "chicago_taxi/start=2023-02-01/end=2023-03-01"


def test_runtime_settings_reject_inverted_window(monkeypatch):
    monkeypatch.setenv("START_DATE", "2023-01-01")
    monkeypatch.setenv("END_DATE", "2023-01-08")
    fn = _function_from_source(DAG_PATH, "_settings_for_run", {
        "replace": replace,
        "get_current_context": lambda: {"params": {
            "extract_start_date": "2023-03-01", "extract_end_date": "2023-02-01"
        }},
    })
    with pytest.raises(ValueError, match="strictly before"):
        fn()


def test_gold_candidates_are_silver_scoped_and_share_publication_guard():
    source = PIPELINE_PATH.read_text(encoding="utf-8")
    assert "staging/" not in source
    assert "_gold_candidates" in source and "_quality_tmp" in source
    make_prefix = _function_from_source(PIPELINE_PATH, "candidate_prefix", {})
    settings = Settings(start_date="2023-02-01", end_date="2023-03-01")
    expected = "silver/chicago_taxi/start=2023-02-01/end=2023-03-01/batch=abc/_gold_candidates"
    assert make_prefix(settings, "abc") == expected
    tree = ast.parse(source)
    for name in ("build_gold_candidate", "publish_gold"):
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                 and n.func.id == "candidate_prefix"]
        assert len(calls) == 1, name
