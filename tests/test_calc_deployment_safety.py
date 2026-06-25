import json
import os
import shutil

import pytest

from TM1_bedrock_py.calc import DeploymentError, Model, StaticMetadataProvider, build_static_cube_metadata
from tests.mock_tm1_service import MockTM1Service


def _simple_provider():
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={
                    "Revenue": "Numeric",
                    "Cost": "Numeric",
                    "Gross Margin": "Numeric",
                },
            ),
        }
    )


def _simple_model(tm1):
    model = Model(metadata_provider=_simple_provider(), tm1=tm1)
    sales = model.cube("Sales")
    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]
    return model


def test_deploy_persists_manifest_with_prior_text_and_diff(tmp_path):
    tm1 = MockTM1Service()
    tm1._cube_rules["sales"] = "['Old Rule'] = N: 1;"
    model = _simple_model(tm1)

    log_dir = str(tmp_path / "deployments")
    preview = model.compile(dry_run=False, deployment_log_dir=log_dir)

    assert preview.deployment["Sales"]["deployed"] is True
    assert preview.deployment["Sales"]["prior_rule_text"] == "['Old Rule'] = N: 1;"
    assert "-['Old Rule'] = N: 1;" in preview.deployment["Sales"]["diff"]
    assert "+SKIPCHECK;" in preview.deployment["Sales"]["diff"]
    assert "Gross Margin" in preview.deployment["Sales"]["diff"]

    assert preview.deployment_manifest_path is not None
    assert os.path.exists(preview.deployment_manifest_path)
    with open(preview.deployment_manifest_path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    assert record["deployment"]["Sales"]["prior_rule_text"] == "['Old Rule'] = N: 1;"


def test_deploy_with_no_prior_rules_records_empty_prior_text(tmp_path):
    tm1 = MockTM1Service()
    model = _simple_model(tm1)

    preview = model.compile(dry_run=False, deployment_log_dir=str(tmp_path))

    assert preview.deployment["Sales"]["prior_rule_text"] == ""


def test_rollback_deployment_restores_prior_rule_text(tmp_path):
    tm1 = MockTM1Service()
    tm1._cube_rules["sales"] = "['Old Rule'] = N: 1;"
    model = _simple_model(tm1)

    preview = model.compile(dry_run=False, deployment_log_dir=str(tmp_path))
    assert tm1.cubes.get("Sales").rules.text != "['Old Rule'] = N: 1;"

    results = model.rollback_deployment(preview.deployment_manifest_path)

    assert results["Sales"]["rolled_back"] is True
    assert tm1.cubes.get("Sales").rules.text == "['Old Rule'] = N: 1;"


def test_rollback_requires_attached_tm1_service(tmp_path):
    tm1 = MockTM1Service()
    model = _simple_model(tm1)
    preview = model.compile(dry_run=False, deployment_log_dir=str(tmp_path))

    detached_model = Model()
    with pytest.raises(DeploymentError):
        detached_model.rollback_deployment(preview.deployment_manifest_path)


def test_rollback_skips_cubes_that_were_never_deployed(tmp_path):
    tm1 = MockTM1Service()
    model = _simple_model(tm1)
    preview = model.compile(dry_run=False, deployment_log_dir=str(tmp_path))

    with open(preview.deployment_manifest_path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    record["deployment"]["Other Cube"] = {"deployed": False, "prior_rule_text": "x"}
    with open(preview.deployment_manifest_path, "w", encoding="utf-8") as handle:
        json.dump(record, handle)

    results = model.rollback_deployment(preview.deployment_manifest_path)
    assert "Other Cube" not in results
