import os
import sys
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

from helper.rm_database_helper import RMDatabaseHelper


class TestNextGenAuAudit:

    def test_insert_navitaire_allocation_audit_nextgen_upsert(self):
        mock_mongo = MagicMock()
        mock_col = MagicMock()
        mock_col.find_one.return_value = None
        mock_mongo.navitaire_allocation_audit = mock_col

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

        doc_id = helper.insertNavitaireAllocationAuditNextGen(
            run_id="test_run_123",
            flight="QP1955 BOMBLR",
            flight_number="1955",
            sector="BOMBLR",
            departure_date="2026-10-19",
            rbd_details=rbd_details,
            au_classes=au_classes,
            au_result=au_result
        )

        assert doc_id is not None
        assert mock_col.update_one.called
        args, kwargs = mock_col.update_one.call_args
        filter_doc = args[0]
        update_doc = args[1]

        assert filter_doc["run_id"] == "test_run_123"
        assert filter_doc["sector"] == "BOMBLR"
        assert filter_doc["flight_number"] == "1955"
        assert filter_doc["departure_date"] == "2026-10-19"

        set_fields = update_doc["$set"]
        assert set_fields["type"] == "nextgen_au"
        assert "newgnwua" not in set_fields
        assert set_fields["flight"] == "QP1955 BOMBLR"
        assert len(set_fields["rbds"]) == 3
        assert set_fields["rbds"][0]["rbd"] == "Y"
        assert set_fields["rbds"][0]["fare"] == 11500.0
        assert set_fields["rbds"][0]["au"] == 180

        assert kwargs.get("upsert") is True

    def test_single_record_consolidation_in_update_rbd(self):
        """Verifies that update-rbd lambda function updates the existing audit record rather than inserting a duplicate."""
        import importlib
        sys.path.insert(0, os.path.abspath("rm-allocation-update-rbd"))
        
        # Mock dependencies for lambda_function import
        mock_col = MagicMock()
        existing_doc = {
            "_id": "doc_existing_123",
            "run_id": "test_run_123",
            "sector": "BOMBLR",
            "flight_number": "1955",
            "departure_date": "2026-10-19",
            "type": "nextgen_au",
            "rbds": [{"rbd": "Y", "fare": 11500.0, "au": 180}]
        }
        mock_col.find_one.return_value = existing_doc

        from unittest.mock import patch
        with patch('requests.Session.post') as mock_post:
            mock_post.return_value.json.return_value = {"data": {"token": "fake_token"}}
            import lambda_function
        lambda_function.navitaire_allocation_audit_col = mock_col

        start_time = datetime.now()
        end_time = datetime.now()
        request_str = json.dumps({"test": "request"})
        result_str = json.dumps({"Y": 180})

        lambda_function.insertNavAudit(
            id="new_random_id",
            startTime=start_time,
            endTime=end_time,
            request=request_str,
            result=result_str,
            runId="test_run_123",
            b2cRunId="b2c_1",
            b2bRunId="b2b_1",
            countOfFailures=0,
            countOfTotal=2,
            navAuditId="doc_existing_123",
            sector="BOMBLR",
            flightNumber="1955",
            departureDate="2026-10-19"
        )

        # Verify it updated the existing record and did NOT call insert_one
        assert mock_col.update_one.called
        assert not mock_col.insert_one.called
        u_args, u_kwargs = mock_col.update_one.call_args
        assert u_args[0] == {"_id": "doc_existing_123"}
        assert u_args[1]["$set"]["run_id"] == "test_run_123"
        assert u_args[1]["$set"]["failures"] == 0

    def test_insert_run_fltDate_audit_upsert(self):
        """Verifies that insert_run_fltDate_audit uses update_one with upsert=True instead of insert_one."""
        import lambda_function
        from pymongo.errors import DuplicateKeyError

        mock_flt_col = MagicMock()
        lambda_function.run_flight_date_audit_col = mock_flt_col

        run_id = "175c9802-a6ca-489c-9719-a02cad572d1e"
        sector = "CCUIXZ"
        flight_number = "1258"
        flight_date = "2026-11-12"
        result = json.dumps({"Y": 180})

        lambda_function.insert_run_fltDate_audit(
            run_id=run_id,
            sector=sector,
            flightNumber=flight_number,
            flightDate=flight_date,
            result=result
        )

        assert mock_flt_col.update_one.called
        assert not mock_flt_col.insert_one.called
        args, kwargs = mock_flt_col.update_one.call_args
        filter_doc = args[0]
        update_doc = args[1]

        assert filter_doc == {
            "runId": run_id,
            "flightNumber": "1258",
            "sector": sector,
            "flightDate": flight_date
        }
        assert update_doc["$set"]["result"] == result
        assert update_doc["$set"]["b2cstatus"] == "Done"
        assert update_doc["$set"]["b2bstatus"] == "Done"
        assert "createdAt" in update_doc["$setOnInsert"]
        assert kwargs.get("upsert") is True

    def test_insert_run_fltDate_audit_duplicate_key_error_fallback(self):
        """Verifies that if upsert raises DuplicateKeyError (race condition), fallback update succeeds."""
        import lambda_function
        from pymongo.errors import DuplicateKeyError

        mock_flt_col = MagicMock()
        # First call raises DuplicateKeyError, second call succeeds
        mock_flt_col.update_one.side_effect = [
            DuplicateKeyError("E11000 duplicate key error"),
            MagicMock()
        ]
        lambda_function.run_flight_date_audit_col = mock_flt_col

        run_id = "175c9802-a6ca-489c-9719-a02cad572d1e"
        sector = "CCUIXZ"
        flight_number = "1258"
        flight_date = "2026-11-12"
        result = json.dumps({"Y": 180})

        # Should not raise DuplicateKeyError
        lambda_function.insert_run_fltDate_audit(
            run_id=run_id,
            sector=sector,
            flightNumber=flight_number,
            flightDate=flight_date,
            result=result
        )

        assert mock_flt_col.update_one.call_count == 2
        # Second call is without upsert
        second_call_kwargs = mock_flt_col.update_one.call_args_list[1][1]
        assert "upsert" not in second_call_kwargs or second_call_kwargs["upsert"] is False
