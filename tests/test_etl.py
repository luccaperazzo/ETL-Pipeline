import pandas as pd
from airflow.dags.etl.etl import validate_and_transform


def test_validate_and_transform_basic():
    data = {
        "order_id": [1, 2, None, 3],
        "customer": ["A", "B", "C", "D"],
        "amount": [10.0, "20", "bad", 30],
        "date": ["2025-01-01", "2025-01-01", "2025-01-01", "bad_date"],
    }
    df = pd.DataFrame(data)
    out = validate_and_transform(df)
    # Expect rows for order 1 and 2 only (3 has bad date, None order_id dropped)
    assert set(out["order_id"].tolist()) == {1, 2}
    assert out["amount"].dtype == float
    assert out["customer"].dtype == object


def test_deduplicate_keep_last():
    data = {
        "order_id": [1, 1],
        "customer": ["Old", "New"],
        "amount": [5, 10],
        "date": ["2025-01-01", "2025-01-02"],
    }
    df = pd.DataFrame(data)
    out = validate_and_transform(df)
    assert len(out) == 1
    assert out.iloc[0]["customer"] == "New"
    assert out.iloc[0]["amount"] == 10.0
