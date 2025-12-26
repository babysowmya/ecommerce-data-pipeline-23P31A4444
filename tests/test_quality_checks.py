import os
import json
import pytest
from scripts.quality_checks.validate_data import run_quality_checks, null_checks, referential_integrity, data_consistency

@pytest.fixture
def quality_report():
    run_quality_checks()
    with open("data/quality/quality_report.json") as f:
        report = json.load(f)
    return report

def test_quality_report_generated():
    assert os.path.exists("data/quality/quality_report.json")

def test_quality_report_structure(quality_report):
    assert "overall_quality_score" in quality_report
    assert "quality_grade" in quality_report
    assert "checks_performed" in quality_report
    assert "null_checks" in quality_report["checks_performed"]
    assert "referential_integrity" in quality_report["checks_performed"]
    assert "data_consistency" in quality_report["checks_performed"]

def null_checks(data):
    failed = sum(1 for row in data if any(v is None for v in row.values()))
    return {"failed": failed}

def referential_integrity(fact_data, dim_data):
    dim_keys = {row["customer_id"] for row in dim_data}
    failed = sum(1 for row in fact_data if row["customer_id"] not in dim_keys)
    return {"failed": failed}

def data_consistency(fact_data, prod_data):
    failed = sum(1 for f, p in zip(fact_data, prod_data) if f["line_total"] != p["line_total"])
    return {"failed": failed}
