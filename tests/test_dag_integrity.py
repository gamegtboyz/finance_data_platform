import pytest
from airflow.models import DagBag

def test_dag_bag_loads_without_errors():
    """Ensure all DAGs in the dags/ directory load without import errors."""
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)

    # Fail immediately if any DAG has import errors
    assert dag_bag.import_errors == {}, (
        f"DAG import errors found:\n"
        + "\n".join(f" {path}: {err}" for path, err in dag_bag.import_errors.items())
    )

def test_stock_pipeline_dag_exists():
    """Verify the stoc k pipeline DAG is registered with the correct dag_id."""
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert "stock_pipeline_dag" in dag_bag.dags


def test_stock_pipeline_dag_structure():
    """Verify the DAG has the expected tasks in the correct order."""
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)
    dag = dag_bag.dags["stock_pipeline_dag"]

    task_ids = set(dag.task_ids)
    assert task_ids == {"extract", "transform", "load"}

    # Verify dependency chain: extract >> transform >> load
    extract = dag.get_task("extract")
    assert "transform" in [t.task_id for t in extract.downstream_list]

    transform = dag.get_task("transform")
    assert "load" in [t.task_id for t in transform.downstream_list]