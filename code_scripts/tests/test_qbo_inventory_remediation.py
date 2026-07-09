from __future__ import annotations

import argparse
import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from code_scripts import qbo_inventory_remediation as remediation


def _adjustment(doc: str, *, qbo_id: str = "1", sync: str = "0", txn_date: str = "2026-04-29"):
    return {
        "Id": qbo_id,
        "SyncToken": sync,
        "DocNumber": doc,
        "TxnDate": txn_date,
        "PrivateNote": f"note {doc}",
        "Line": [
            {
                "ItemAdjustmentLineDetail": {
                    "ItemRef": {"value": "10", "name": "Widget"},
                    "QtyDiff": 5,
                }
            }
        ],
    }


class QboInventoryRemediationTests(unittest.TestCase):
    def _cfg(self):
        return SimpleNamespace(company_key="company_a", realm_id="realm-1")

    def _args(self, td: Path, *, command: str = "plan", apply: bool = False, confirm: bool = False):
        return argparse.Namespace(
            command=command,
            company="company_a",
            from_date="2026-04-29",
            to_date="2026-04-30",
            number_prefix="INVCON",
            candidate_csv=None,
            exclude_number=[],
            max_transactions=None,
            min_impact=None,
            output_dir=td,
            allow_invadj=False,
            apply=apply,
            confirm_delete_inventory_adjustments=confirm,
            fail_fast=False,
        )

    def test_plan_mode_never_calls_delete(self):
        with tempfile.TemporaryDirectory() as td:
            args = self._args(Path(td), command="plan")
            with mock.patch.object(remediation, "load_company_config", return_value=self._cfg()), \
                 mock.patch.object(remediation, "verify_realm_match"), \
                 mock.patch.object(remediation, "TokenManager", return_value=mock.Mock()), \
                 mock.patch.object(remediation, "query_inventory_adjustments", return_value=[_adjustment("INVCON-20260429-9275")]), \
                 mock.patch.object(remediation, "fetch_full_inventory_adjustments", side_effect=lambda _tm, _realm, rows: rows), \
                 mock.patch.object(remediation, "delete_inventory_adjustment") as delete_mock:
                exit_code = remediation.run(args)

        self.assertEqual(exit_code, 0)
        delete_mock.assert_not_called()

    def test_apply_mode_requires_confirmation_flag(self):
        with tempfile.TemporaryDirectory() as td:
            args = self._args(Path(td), command="delete", apply=True, confirm=False)
            with self.assertRaisesRegex(ValueError, "requires --apply"):
                remediation._validate_args(args)

    def test_excluded_transaction_numbers_are_not_deleted(self):
        plan = remediation.build_plan(
            company_key="company_a",
            realm_id="realm-1",
            qbo_adjustments=[_adjustment("INVCON-20260430-14620")],
            candidate_rows=None,
            number_prefix="INVCON",
            exclude_numbers=set(remediation.DEFAULT_EXCLUDED_NUMBERS),
            max_transactions=None,
            min_impact=None,
            allow_invadj=False,
        )
        self.assertEqual(plan[0]["action"], "excluded")

        with mock.patch.object(remediation, "delete_inventory_adjustment") as delete_mock:
            results = remediation.apply_deletions(
                token_mgr=mock.Mock(),
                realm_id="realm-1",
                plan_rows=plan,
            )
        self.assertEqual(results, [])
        delete_mock.assert_not_called()

    def test_already_deleted_candidate_is_skipped_when_not_found(self):
        plan = remediation.build_plan(
            company_key="company_a",
            realm_id="realm-1",
            qbo_adjustments=[],
            candidate_rows=[{"doc_number": "INVCON-20260429-9275", "expected_impact": "100"}],
            number_prefix="INVCON",
            exclude_numbers=set(),
            max_transactions=None,
            min_impact=None,
            allow_invadj=False,
            token_mgr=None,
        )
        self.assertEqual(plan[0]["action"], "already_missing")
        self.assertEqual(plan[0]["reason"], "not_found_in_qbo")

    def test_max_transactions_limits_candidates(self):
        plan = remediation.build_plan(
            company_key="company_a",
            realm_id="realm-1",
            qbo_adjustments=[
                _adjustment("INVCON-20260429-9275", qbo_id="1"),
                _adjustment("INVCON-20260429-9285", qbo_id="2"),
            ],
            candidate_rows=None,
            number_prefix="INVCON",
            exclude_numbers=set(),
            max_transactions=1,
            min_impact=None,
            allow_invadj=False,
        )
        self.assertEqual([row["action"] for row in plan].count("delete_candidate"), 1)
        self.assertEqual([row["reason"] for row in plan].count("max_transactions_limit"), 1)

    def test_invadj_is_refused_by_default(self):
        plan = remediation.build_plan(
            company_key="company_a",
            realm_id="realm-1",
            qbo_adjustments=[_adjustment("INVADJ-20260430-1")],
            candidate_rows=None,
            number_prefix="INVCON",
            exclude_numbers=set(),
            max_transactions=None,
            min_impact=None,
            allow_invadj=False,
        )
        self.assertEqual(plan[0]["action"], "skipped")
        self.assertEqual(plan[0]["reason"], "invadj_refused_by_default")

        with self.assertRaisesRegex(ValueError, "Refusing to target INVADJ"):
            remediation.build_plan(
                company_key="company_a",
                realm_id="realm-1",
                qbo_adjustments=[],
                candidate_rows=[],
                number_prefix="INVADJ",
                exclude_numbers=set(),
                max_transactions=None,
                min_impact=None,
                allow_invadj=False,
            )

    def test_candidate_csv_sorting_by_expected_impact(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "candidates.csv"
            with open(path, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["doc_number", "expected_impact"])
                writer.writeheader()
                writer.writerow({"doc_number": "INVCON-LOW", "expected_impact": "100"})
                writer.writerow({"doc_number": "INVCON-HIGH", "expected_impact": "500"})
            rows = remediation.load_candidate_csv(path)

        with mock.patch.object(
            remediation,
            "query_inventory_adjustments_by_doc_numbers",
            return_value={
                "INVCON-LOW": _adjustment("INVCON-LOW", qbo_id="1"),
                "INVCON-HIGH": _adjustment("INVCON-HIGH", qbo_id="2"),
            },
        ):
            plan = remediation.build_plan(
                company_key="company_a",
                realm_id="realm-1",
                qbo_adjustments=[],
                candidate_rows=rows,
                number_prefix="INVCON",
                exclude_numbers=set(),
                max_transactions=None,
                min_impact=None,
                allow_invadj=False,
                token_mgr=mock.Mock(),
            )

        self.assertEqual([row["doc_number"] for row in plan], ["INVCON-HIGH", "INVCON-LOW"])

    def test_deletion_failures_are_logged_and_fail_fast_stops_batch(self):
        plan_rows = [
            {
                **remediation._plan_row_from_adjustment(
                    company_key="company_a",
                    realm_id="realm-1",
                    adjustment=_adjustment("INVCON-1", qbo_id="1", sync="0"),
                    doc_number="INVCON-1",
                    action="delete_candidate",
                    reason="matches_remediation_criteria",
                    status="planned",
                ),
                "sync_token": "0",
            },
            {
                **remediation._plan_row_from_adjustment(
                    company_key="company_a",
                    realm_id="realm-1",
                    adjustment=_adjustment("INVCON-2", qbo_id="2", sync="0"),
                    doc_number="INVCON-2",
                    action="delete_candidate",
                    reason="matches_remediation_criteria",
                    status="planned",
                ),
                "sync_token": "0",
            },
        ]

        with mock.patch.object(
            remediation,
            "delete_inventory_adjustment",
            side_effect=[(False, "boom"), (True, "")],
        ):
            results = remediation.apply_deletions(
                token_mgr=mock.Mock(),
                realm_id="realm-1",
                plan_rows=plan_rows,
                fail_fast=False,
            )
        self.assertEqual([r["result"] for r in results], ["failed", "deleted"])

        with mock.patch.object(
            remediation,
            "delete_inventory_adjustment",
            side_effect=[(False, "boom"), (True, "")],
        ):
            results = remediation.apply_deletions(
                token_mgr=mock.Mock(),
                realm_id="realm-1",
                plan_rows=plan_rows,
                fail_fast=True,
            )
        self.assertEqual([r["result"] for r in results], ["failed"])
