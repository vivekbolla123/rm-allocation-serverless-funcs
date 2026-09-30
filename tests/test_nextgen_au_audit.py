import os
import sys
import json
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

from helper.rm_database_helper import RMDatabaseHelper


class TestNextGenAuAudit:

    def test_insert_navitaire_allocation_audit_nextgen(self):
        mock_mongo = MagicMock()
        helper = RMDatabaseHelper(
            mongo_db=mock_mongo,
            dwconn=MagicMock(),
            runId="test_run_123",
            inputSource=MagicMock(),
            debugmode=True,
            cache_client=MagicMock(),
            currParams=MagicMock()
        )

        rbd_details = [
            {"rbd": "Y", "fare": 11500.0, "au": 180},
            {"rbd": "B", "fare": 10200.0, "au": 160},
            {"rbd": "C", "fare": 9000.0, "au": 140}
        ]
        au_classes = {
            "Y": {"classOfService": "Y", "classType": "B2C", "classAU": 180, "fare": 11500.0},
            "B": {"classOfService": "B", "classType": "B2C", "classAU": 160, "fare": 10200.0}
        }
        au_result = {"Y": 180, "B": 160, "C": 140}

        helper.insertNavitaireAllocationAuditNextGen(
            run_id="test_run_123",
            flight="QP1955 BOMBLR",
            flight_number="1955",
            sector="BOMBLR",
            departure_date="2026-10-19",
            rbd_details=rbd_details,
            au_classes=au_classes,
            au_result=au_result
        )

        assert mock_mongo.navitaire_allocation_audit.insert_one.called
        args, kwargs = mock_mongo.navitaire_allocation_audit.insert_one.call_args
        inserted_doc = args[0]

        assert inserted_doc["run_id"] == "test_run_123"
        assert inserted_doc["type"] == "nextgen_au"
        assert inserted_doc["flight"] == "QP1955 BOMBLR"
        assert inserted_doc["flight_number"] == "1955"
        assert inserted_doc["sector"] == "BOMBLR"
        assert inserted_doc["departure_date"] == "2026-10-19"
        assert len(inserted_doc["rbds"]) == 3
        assert inserted_doc["rbds"][0]["rbd"] == "Y"
        assert inserted_doc["rbds"][0]["fare"] == 11500.0
        assert inserted_doc["rbds"][0]["au"] == 180

        # Check request JSON string
        request_obj = json.loads(inserted_doc["request"])
        assert request_obj["runId"] == "test_run_123"
        assert request_obj["type"] == "nextgen_au"
        assert request_obj["auClasses"]["Y"]["classAU"] == 180
