"""Host-runnable: only exercises pure key-building logic, no real MinIO
client needed.
"""
from extract.minio_client import bronze_key


def test_bronze_key_is_unique_across_rapid_successive_calls():
    """Regression test: extract/run.py calls put_json once per INEGI
    indicator in a tight loop, easily landing two calls within the same
    second — a second-precision-only timestamp key silently let the
    second call's bronze write overwrite the first's. Generate a lot of
    keys back-to-back (worst case for a timestamp collision) and check
    none collide."""
    keys = {bronze_key("inegi") for _ in range(200)}
    assert len(keys) == 200


def test_bronze_key_shape():
    key = bronze_key("banxico")
    assert key.startswith("banxico/banxico_")
    assert key.endswith(".json")
