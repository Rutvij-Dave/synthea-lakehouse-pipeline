import os
import pytest

sql = pytest.importorskip("databricks.sql")

CATALOG = "claims_lakehouse"

@pytest.fixture(scope="module")
def conn():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not all([host, token, warehouse]):
        pytest.skip("Databricks secrets not configured")
    return sql.connect(
        server_hostname=host.replace("https://",""),
        http_path=f"/sql/1.0/warehouses/{warehouse}",
        access_token=token,
    )

def scalar(conn, q):
    with conn.cursor() as c:
        c.execute(q)
        return c.fetchone()[0]

def test_silver_claim_grain(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.silver.silver_claim_dedup") ==            scalar(conn, f"SELECT COUNT(DISTINCT claim_id) FROM {CATALOG}.silver.silver_claim_dedup")

def test_gold_claim_grain(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.gold.fact_claim") ==            scalar(conn, f"SELECT COUNT(DISTINCT claim_id) FROM {CATALOG}.gold.fact_claim")

def test_ai_mv_exists_and_unique(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.gold.mv_ai_claim_features") > 0
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.gold.mv_ai_claim_features") ==            scalar(conn, f"SELECT COUNT(DISTINCT claim_id) FROM {CATALOG}.gold.mv_ai_claim_features")

def test_claim_amount_non_negative(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.gold.fact_claim WHERE claim_total_amount < 0") == 0

def test_governance_objects(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.governance.data_dictionary") > 0
    assert scalar(conn, f"SELECT COUNT(*) FROM {CATALOG}.governance.dq_governance") > 0
