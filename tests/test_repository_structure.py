from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

def test_layers_exist():
    for d in ["00_landing","01_bronze","02_silver","03_gold","04_reporting","05_governance","tests"]:
        assert (ROOT / d).is_dir(), f"Missing layer: {d}"

def test_ai_materialized_view_file_exists():
    assert (ROOT / "03_gold" / "13_mv_ai_claim_features.sql").exists()

def test_github_actions_exists():
    assert any((ROOT / ".github" / "workflows").glob("*.yml"))

def test_governance_tests_exist():
    assert (ROOT / "tests" / "governance_tests.yml").exists()
