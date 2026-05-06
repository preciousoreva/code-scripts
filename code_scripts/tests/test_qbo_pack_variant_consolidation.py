import csv
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest import mock

from code_scripts import qbo_pack_variant_consolidation as consolidation
from code_scripts.qbo_pack_variant_consolidation import (
    build_consolidation_plan,
    build_doc_number,
    build_lines_from_plan_row,
    build_private_note,
    is_duplicate_doc_number_error,
    write_report,
    _classify_for_apply,
    _REPORT_FIELDS,
)


# Each helper row mimics the shape produced by inventory_sync.load_qbo_inventory_item_rows.
def _qbo_row(item_id, name, base_name, has_pack, qty):
    return {
        "Id": str(item_id),
        "Name": name,
        "base_name": base_name,
        "qbo_has_pack": bool(has_pack),
        "qbo_qty_on_hand": float(qty),
    }


# ---------------------------------------------------------------------------
# TROPHY scenario (the canonical example from the requirements)
# ---------------------------------------------------------------------------


class TrophyScenarioTest(unittest.TestCase):
    def setUp(self):
        # 3 items in QBO: 1 base + 2 pack variants
        self.qbo_rows = [
            _qbo_row(9364, "TROPHY LAGER CAN 500ML", "TROPHY LAGER CAN 500ML", has_pack=False, qty=-295),
            _qbo_row(9365, "TROPHY LAGER CAN 500ML*12", "TROPHY LAGER CAN 500ML", has_pack=True, qty=3),
            _qbo_row(9366, "TROPHY LAGER CAN 500ML*24", "TROPHY LAGER CAN 500ML", has_pack=True, qty=52),
        ]
        # EPOS: 14 packs of *24 -> 14 * 24 = 336 single units
        self.epos_targets = {"trophy lager can 500ml": 336.0}

    def test_trophy_consolidation_plan(self):
        plan = build_consolidation_plan(
            self.qbo_rows, self.epos_targets, company_key="company_a"
        )
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["consolidation_recommended_action"], "consolidation_plan_available")
        self.assertEqual(r["risk_reason"], "")
        self.assertEqual(r["base_name"], "TROPHY LAGER CAN 500ML")
        self.assertEqual(r["epos_single_units_target"], "336")
        self.assertEqual(r["base_qbo_item_id"], "9364")
        self.assertEqual(r["base_qbo_qty_on_hand"], -295.0)
        # 336 - (-295) = 631
        self.assertEqual(r["base_qty_diff_to_target"], 631.0)

    def test_trophy_pack_variants_listed_in_order(self):
        plan = build_consolidation_plan(
            self.qbo_rows, self.epos_targets, company_key="company_a"
        )
        r = plan[0]
        self.assertEqual(r["pack_variant_item_ids"], "9365|9366")
        self.assertEqual(
            r["pack_variant_names"],
            "TROPHY LAGER CAN 500ML*12|TROPHY LAGER CAN 500ML*24",
        )
        self.assertEqual(r["pack_variant_qtys_on_hand"], "3|52")
        # Diff to zero = -current
        self.assertEqual(r["pack_variant_qty_diffs_to_zero"], "-3|-52")

    def test_trophy_total_simple_sum(self):
        plan = build_consolidation_plan(
            self.qbo_rows, self.epos_targets, company_key="company_a"
        )
        r = plan[0]
        # -295 + 3 + 52 = -240
        self.assertEqual(r["total_qbo_qty_before_simple_sum"], -240.0)

    def test_trophy_planned_line_count(self):
        plan = build_consolidation_plan(
            self.qbo_rows, self.epos_targets, company_key="company_a"
        )
        # 1 base line + 2 pack variant lines
        self.assertEqual(plan[0]["planned_line_count"], 3)


# ---------------------------------------------------------------------------
# Manual-review cases
# ---------------------------------------------------------------------------


class NoBaseTest(unittest.TestCase):
    def test_pack_variants_without_active_base_yields_manual_review(self):
        rows = [
            _qbo_row(2, "WIDGET 330ml*12", "WIDGET 330ml", has_pack=True, qty=5),
        ]
        plan = build_consolidation_plan(
            rows, {"widget 330ml": 100.0}, company_key="company_a"
        )
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["consolidation_recommended_action"], "needs_manual_review")
        self.assertEqual(r["risk_reason"], "no_active_exact_base_in_qbo")
        self.assertEqual(r["base_qbo_item_id"], "")
        self.assertEqual(r["base_qbo_qty_on_hand"], "")
        self.assertEqual(r["base_qty_diff_to_target"], "")
        self.assertEqual(r["planned_line_count"], 0)
        # Pack diagnostics still populated
        self.assertEqual(r["pack_variant_item_ids"], "2")
        self.assertEqual(r["pack_variant_qtys_on_hand"], "5")


class MultipleBaseTest(unittest.TestCase):
    def test_multiple_active_exact_bases_yields_manual_review(self):
        rows = [
            _qbo_row(1, "WIDGET 330ml", "WIDGET 330ml", has_pack=False, qty=10),
            _qbo_row(99, "WIDGET 330ml", "WIDGET 330ml", has_pack=False, qty=4),
            _qbo_row(2, "WIDGET 330ml*12", "WIDGET 330ml", has_pack=True, qty=5),
        ]
        plan = build_consolidation_plan(
            rows, {"widget 330ml": 100.0}, company_key="company_a"
        )
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["consolidation_recommended_action"], "needs_manual_review")
        self.assertEqual(r["risk_reason"], "multiple_active_exact_base_in_qbo")
        self.assertEqual(r["base_qbo_item_id"], "1|99")
        self.assertEqual(r["base_qbo_qty_on_hand"], "10|4")
        self.assertEqual(r["base_qty_diff_to_target"], "")
        self.assertEqual(r["planned_line_count"], 0)


class NoEposTargetTest(unittest.TestCase):
    def test_base_with_packs_but_no_epos_target_yields_manual_review(self):
        rows = [
            _qbo_row(1, "WIDGET 330ml", "WIDGET 330ml", has_pack=False, qty=10),
            _qbo_row(2, "WIDGET 330ml*12", "WIDGET 330ml", has_pack=True, qty=5),
        ]
        plan = build_consolidation_plan(rows, epos_targets={}, company_key="company_a")
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["consolidation_recommended_action"], "needs_manual_review")
        self.assertEqual(r["risk_reason"], "no_epos_target")
        self.assertEqual(r["base_qbo_item_id"], "1")
        self.assertEqual(r["base_qbo_qty_on_hand"], 10.0)
        self.assertEqual(r["base_qty_diff_to_target"], "")
        self.assertEqual(r["epos_single_units_target"], "")
        self.assertEqual(r["planned_line_count"], 0)


# ---------------------------------------------------------------------------
# Skip cases
# ---------------------------------------------------------------------------


class SkippedScenariosTest(unittest.TestCase):
    def test_base_without_packs_is_not_in_plan(self):
        # Bases without pack variants don't need consolidation — skip entirely.
        rows = [
            _qbo_row(1, "WIDGET 330ml", "WIDGET 330ml", has_pack=False, qty=10),
        ]
        plan = build_consolidation_plan(
            rows, {"widget 330ml": 100.0}, company_key="company_a"
        )
        self.assertEqual(plan, [])

    def test_in_scope_bases_filter_skips_others(self):
        rows = [
            _qbo_row(1, "ALPHA", "ALPHA", has_pack=False, qty=10),
            _qbo_row(2, "ALPHA*6", "ALPHA", has_pack=True, qty=2),
            _qbo_row(3, "BETA", "BETA", has_pack=False, qty=5),
            _qbo_row(4, "BETA*12", "BETA", has_pack=True, qty=1),
        ]
        plan = build_consolidation_plan(
            rows,
            {"alpha": 50.0, "beta": 75.0},
            company_key="company_a",
            in_scope_bases={"alpha"},
        )
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["base_name"], "ALPHA")


# ---------------------------------------------------------------------------
# Math
# ---------------------------------------------------------------------------


class MathTest(unittest.TestCase):
    def test_diff_to_target_positive_when_target_above_current(self):
        rows = [
            _qbo_row(1, "P", "P", has_pack=False, qty=5),
            _qbo_row(2, "P*6", "P", has_pack=True, qty=3),
        ]
        plan = build_consolidation_plan(rows, {"p": 100.0}, company_key="c")
        self.assertEqual(plan[0]["base_qty_diff_to_target"], 95.0)

    def test_diff_to_target_negative_when_target_below_current(self):
        rows = [
            _qbo_row(1, "P", "P", has_pack=False, qty=200),
            _qbo_row(2, "P*6", "P", has_pack=True, qty=3),
        ]
        plan = build_consolidation_plan(rows, {"p": 50.0}, company_key="c")
        self.assertEqual(plan[0]["base_qty_diff_to_target"], -150.0)

    def test_pack_diff_zero_when_pack_qty_already_zero(self):
        rows = [
            _qbo_row(1, "P", "P", has_pack=False, qty=10),
            _qbo_row(2, "P*6", "P", has_pack=True, qty=0),
        ]
        plan = build_consolidation_plan(rows, {"p": 50.0}, company_key="c")
        self.assertEqual(plan[0]["pack_variant_qty_diffs_to_zero"], "0")
        # Still consolidation_plan_available — base diff may still be nonzero
        self.assertEqual(plan[0]["consolidation_recommended_action"], "consolidation_plan_available")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class WriteReportTest(unittest.TestCase):
    def test_report_has_expected_columns_and_trophy_values(self):
        rows = [
            _qbo_row(9364, "TROPHY LAGER CAN 500ML", "TROPHY LAGER CAN 500ML", has_pack=False, qty=-295),
            _qbo_row(9365, "TROPHY LAGER CAN 500ML*12", "TROPHY LAGER CAN 500ML", has_pack=True, qty=3),
            _qbo_row(9366, "TROPHY LAGER CAN 500ML*24", "TROPHY LAGER CAN 500ML", has_pack=True, qty=52),
        ]
        plan = build_consolidation_plan(
            rows, {"trophy lager can 500ml": 336.0}, company_key="company_a"
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "plan.csv"
            write_report(plan, path)
            with open(path, newline="", encoding="utf-8") as h:
                reader = csv.DictReader(h)
                fields = reader.fieldnames
                rows_out = list(reader)

        for required in _REPORT_FIELDS:
            self.assertIn(required, fields)
        self.assertEqual(rows_out[0]["base_qbo_item_id"], "9364")
        self.assertEqual(rows_out[0]["base_qty_diff_to_target"], "631.0")
        self.assertEqual(rows_out[0]["pack_variant_item_ids"], "9365|9366")
        self.assertEqual(rows_out[0]["pack_variant_qty_diffs_to_zero"], "-3|-52")
        self.assertEqual(rows_out[0]["epos_single_units_target"], "336")
        self.assertEqual(rows_out[0]["planned_line_count"], "3")
        self.assertEqual(
            rows_out[0]["consolidation_recommended_action"], "consolidation_plan_available"
        )


# ---------------------------------------------------------------------------
# Apply / dry-run: payload + safety cap unit tests
# ---------------------------------------------------------------------------


_TROPHY_PLAN_ROW = {
    "company_key": "company_a",
    "base_name": "TROPHY LAGER CAN 500ML",
    "epos_single_units_target": "336",
    "base_qbo_item_id": "9364",
    "base_qbo_name": "TROPHY LAGER CAN 500ML",
    "base_qbo_qty_on_hand": -295.0,
    "base_qty_diff_to_target": 631.0,
    "pack_variant_item_ids": "9365|9366",
    "pack_variant_names": "TROPHY LAGER CAN 500ML*12|TROPHY LAGER CAN 500ML*24",
    "pack_variant_qtys_on_hand": "3|52",
    "pack_variant_qty_diffs_to_zero": "-3|-52",
    "total_qbo_qty_before_simple_sum": -240.0,
    "planned_line_count": 3,
    "consolidation_recommended_action": "consolidation_plan_available",
    "risk_reason": "",
}


class TrophyPayloadTest(unittest.TestCase):
    def test_lines_match_expected_diffs(self):
        lines = build_lines_from_plan_row(_TROPHY_PLAN_ROW)
        # Order: base first, then pack variants in file order.
        self.assertEqual(lines, [
            {"item_id": "9364", "qty_diff": 631.0},
            {"item_id": "9365", "qty_diff": -3.0},
            {"item_id": "9366", "qty_diff": -52.0},
        ])

    def test_zero_diffs_are_dropped_from_lines(self):
        row = dict(_TROPHY_PLAN_ROW)
        row["base_qty_diff_to_target"] = 0  # base already at target
        lines = build_lines_from_plan_row(row)
        ids = [l["item_id"] for l in lines]
        self.assertNotIn("9364", ids)  # zero base line dropped
        self.assertIn("9365", ids)
        self.assertIn("9366", ids)

    def test_private_note_includes_required_metadata(self):
        note = build_private_note(_TROPHY_PLAN_ROW, scope_description="category=ALCOHOLS & SPIRITS")
        self.assertIn("OIAT pack variant consolidation", note)
        self.assertIn("base: TROPHY LAGER CAN 500ML", note)
        self.assertIn("base item id: 9364", note)
        self.assertIn("EPOS single-unit target: 336", note)
        self.assertIn("pack item ids: 9365, 9366", note)
        self.assertIn("scope: category=ALCOHOLS & SPIRITS", note)


class DuplicateDocNumberDetectionTest(unittest.TestCase):
    def test_detects_qbo_code_6240(self):
        exc = RuntimeError("InventoryAdjustment failed: HTTP 400: ... code: '6240' ...")
        self.assertTrue(is_duplicate_doc_number_error(exc))

    def test_detects_english_phrase_case_insensitive(self):
        exc = RuntimeError("Some QBO message including Duplicate Document Number Error")
        self.assertTrue(is_duplicate_doc_number_error(exc))

    def test_returns_false_for_unrelated_errors(self):
        self.assertFalse(is_duplicate_doc_number_error(RuntimeError("HTTP 401: token expired")))
        self.assertFalse(is_duplicate_doc_number_error(ValueError("missing item_id")))


class DocNumberTest(unittest.TestCase):
    def test_trophy_doc_number_matches_required_format(self):
        # Required by QBO: payload must carry a non-null DocNumber.
        # Format: INVCON-YYYYMMDD-{base_item_id}
        self.assertEqual(
            build_doc_number(txn_date="2026-04-27", base_item_id="9364"),
            "INVCON-20260427-9364",
        )

    def test_doc_number_is_deterministic_per_date_and_base(self):
        a = build_doc_number(txn_date="2026-04-27", base_item_id="9364")
        b = build_doc_number(txn_date="2026-04-27", base_item_id="9364")
        self.assertEqual(a, b)

    def test_doc_number_changes_with_date(self):
        d1 = build_doc_number(txn_date="2026-04-27", base_item_id="9364")
        d2 = build_doc_number(txn_date="2026-04-28", base_item_id="9364")
        self.assertNotEqual(d1, d2)

    def test_doc_number_changes_with_base_item(self):
        a = build_doc_number(txn_date="2026-04-27", base_item_id="9364")
        b = build_doc_number(txn_date="2026-04-27", base_item_id="9999")
        self.assertNotEqual(a, b)

    def test_doc_number_empty_when_inputs_missing(self):
        self.assertEqual(build_doc_number("", "9364"), "")
        self.assertEqual(build_doc_number("2026-04-27", ""), "")


class SafetyCapTest(unittest.TestCase):
    def _row(self, action="consolidation_plan_available", base_diff=10, line_count=3, name="P"):
        r = dict(_TROPHY_PLAN_ROW)
        r["base_name"] = name
        r["consolidation_recommended_action"] = action
        r["risk_reason"] = "" if action == "consolidation_plan_available" else "no_active_exact_base_in_qbo"
        r["base_qty_diff_to_target"] = base_diff
        r["planned_line_count"] = line_count
        return r

    def test_blocks_row_over_max_abs_base_diff(self):
        rows = [self._row(base_diff=2000)]
        postable, blocked = _classify_for_apply(rows, max_abs_base_diff=1000.0, max_lines=10)
        self.assertEqual(postable, [])
        self.assertEqual(len(blocked), 1)
        self.assertIn("max-abs-base-diff", blocked[0][1])

    def test_blocks_row_over_max_lines(self):
        rows = [self._row(line_count=15)]
        postable, blocked = _classify_for_apply(rows, max_abs_base_diff=10000.0, max_lines=10)
        self.assertEqual(postable, [])
        self.assertEqual(len(blocked), 1)
        self.assertIn("max-lines", blocked[0][1])

    def test_excludes_rows_with_other_actions(self):
        rows = [
            self._row(action="consolidation_plan_available", name="OK"),
            self._row(action="needs_manual_review", name="REVIEW"),
        ]
        postable, blocked = _classify_for_apply(rows, max_abs_base_diff=10000.0, max_lines=100)
        names = [r["base_name"] for r in postable]
        self.assertEqual(names, ["OK"])
        self.assertEqual(blocked, [])

    def test_negative_base_diff_compared_by_absolute_value(self):
        rows = [self._row(base_diff=-1500)]
        postable, blocked = _classify_for_apply(rows, max_abs_base_diff=1000.0, max_lines=10)
        self.assertEqual(postable, [])
        self.assertEqual(len(blocked), 1)


# ---------------------------------------------------------------------------
# main() guard / wiring tests
# ---------------------------------------------------------------------------


class _FakeConfig:
    def __init__(self, adjust_account_id=None):
        self.company_key = "company_a"
        self.realm_id = "REALM"
        self.display_name = "Company A"
        self.inventory_adjustment_account_id = adjust_account_id


def _stub_qbo_df(rows):
    class _Stub:
        def to_dict(self, orient="records"):
            return list(rows)

    return _Stub()


def _stub_epos_df(rows):
    """Return an object that mimics a pandas DataFrame's iterrows() interface."""

    class _Stub:
        def __init__(self, rs):
            self._rs = rs

        def iterrows(self):
            for i, r in enumerate(self._rs):
                yield i, r

    return _Stub(rows)


_QBO_TROPHY_ROWS = [
    {"Id": "9364", "Name": "TROPHY LAGER CAN 500ML", "base_name": "TROPHY LAGER CAN 500ML",
     "qbo_has_pack": False, "qbo_qty_on_hand": -295.0},
    {"Id": "9365", "Name": "TROPHY LAGER CAN 500ML*12", "base_name": "TROPHY LAGER CAN 500ML",
     "qbo_has_pack": True, "qbo_qty_on_hand": 3.0},
    {"Id": "9366", "Name": "TROPHY LAGER CAN 500ML*24", "base_name": "TROPHY LAGER CAN 500ML",
     "qbo_has_pack": True, "qbo_qty_on_hand": 52.0},
]
_EPOS_TROPHY_ROWS = [{"base_name": "TROPHY LAGER CAN 500ML", "epos_single_units": 336.0,
                     "epos_categories": "ALCOHOLS & SPIRITS"}]


def _patch_main_dependencies(*, qbo_rows, epos_rows, adjust_account_id=None):
    """Yield a list of mock context managers for main()'s deps."""
    return [
        mock.patch.object(
            consolidation, "load_company_config",
            return_value=_FakeConfig(adjust_account_id=adjust_account_id),
        ),
        mock.patch.object(consolidation, "ensure_company_runtime_compatible"),
        mock.patch.object(
            consolidation, "get_available_companies", return_value=["company_a"],
        ),
        mock.patch.object(
            consolidation, "_resolve_qbo_csv", return_value=Path("/dev/null"),
        ),
        mock.patch.object(
            consolidation, "load_qbo_inventory_item_rows",
            return_value=_stub_qbo_df(qbo_rows),
        ),
        mock.patch.object(
            consolidation, "load_epos_stock_snapshot",
            return_value=_stub_epos_df(epos_rows),
        ),
        mock.patch.object(consolidation, "verify_realm_match"),
    ]


class MainGuardTest(unittest.TestCase):
    def setUp(self):
        self._inventory_apply_env = mock.patch.dict(
            os.environ,
            {"OIAT_ALLOW_INVENTORY_APPLY": "true"},
            clear=False,
        )
        self._inventory_apply_env.start()
        self.addCleanup(self._inventory_apply_env.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, argv, *, qbo_rows=None, epos_rows=None, adjust_account_id=None):
        out_path = self.tmp / "report.csv"
        full_argv = list(argv) + ["--output", str(out_path)]
        patches = _patch_main_dependencies(
            qbo_rows=qbo_rows or _QBO_TROPHY_ROWS,
            epos_rows=epos_rows or _EPOS_TROPHY_ROWS,
            adjust_account_id=adjust_account_id,
        )
        for p in patches:
            p.start()
        try:
            buf, err = io.StringIO(), io.StringIO()
            with redirect_stdout(buf), redirect_stderr(err):
                rc = consolidation.main(full_argv)
            return rc, buf.getvalue(), err.getvalue()
        finally:
            for p in reversed(patches):
                p.stop()

    def test_apply_requires_max_products(self):
        rc, _, err = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--product", "TROPHY",
        ], adjust_account_id="82")
        self.assertEqual(rc, 2)
        self.assertIn("--apply requires --max-products", err)

    def test_apply_rejects_zero_max_products(self):
        rc, _, err = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "0", "--product", "TROPHY",
        ], adjust_account_id="82")
        self.assertEqual(rc, 2)
        self.assertIn("--max-products must be > 0", err)

    def test_apply_requires_product_or_category_scope(self):
        rc, _, err = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "1",
        ], adjust_account_id="82")
        self.assertEqual(rc, 2)
        self.assertIn("--product or --category", err)

    def test_apply_blocks_when_no_adjust_account_configured(self):
        rc, _, err = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "1", "--product", "TROPHY",
        ], adjust_account_id=None)
        self.assertEqual(rc, 2)
        self.assertIn("inventory_adjustment_account_id is not configured", err)

    def test_apply_and_dry_run_together_returns_2(self):
        rc, _, err = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "1", "--product", "TROPHY", "--dry-run",
        ], adjust_account_id="82")
        self.assertEqual(rc, 2)
        self.assertIn("--apply or --dry-run", err)


class DryRunPayloadTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, argv, *, adjust_account_id="82"):
        out_path = self.tmp / "report.csv"
        full_argv = list(argv) + ["--output", str(out_path)]
        patches = _patch_main_dependencies(
            qbo_rows=_QBO_TROPHY_ROWS,
            epos_rows=_EPOS_TROPHY_ROWS,
            adjust_account_id=adjust_account_id,
        )
        for p in patches:
            p.start()
        try:
            buf, err = io.StringIO(), io.StringIO()
            with mock.patch.object(consolidation, "post_inventory_adjustment") as post_mock, \
                 redirect_stdout(buf), redirect_stderr(err):
                rc = consolidation.main(full_argv)
            return rc, buf.getvalue(), err.getvalue(), post_mock
        finally:
            for p in reversed(patches):
                p.stop()

    def test_dry_run_builds_trophy_payload_and_does_not_post(self):
        rc, out, _err, post_mock = self._run([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--dry-run", "--product", "TROPHY", "--txn-date", "2026-04-27",
        ])
        self.assertEqual(rc, 0)
        post_mock.assert_not_called()

        # Extract the JSON payload line emitted by [DRY-RUN-PLAN]
        payload_line = next(
            (line for line in out.splitlines() if line.strip().startswith("payload=")),
            "",
        )
        self.assertTrue(payload_line, msg=f"no payload line in output:\n{out}")
        import json as _json
        payload = _json.loads(payload_line.strip().split("payload=", 1)[1])

        # Expected TROPHY payload from the requirements
        self.assertEqual(payload["TxnDate"], "2026-04-27")
        self.assertEqual(payload["AdjustAccountRef"]["value"], "82")
        # QBO requires a non-null DocNumber; we generate a deterministic one.
        self.assertEqual(payload["DocNumber"], "INVCON-20260427-9364")
        line_pairs = [
            (l["ItemAdjustmentLineDetail"]["ItemRef"]["value"],
             l["ItemAdjustmentLineDetail"]["QtyDiff"])
            for l in payload["Line"]
        ]
        self.assertEqual(line_pairs, [
            ("9364", 631.0),
            ("9365", -3.0),
            ("9366", -52.0),
        ])
        # PrivateNote sanity
        self.assertIn("OIAT pack variant consolidation", payload["PrivateNote"])
        self.assertIn("9364", payload["PrivateNote"])


class ApplyEndToEndTest(unittest.TestCase):
    def setUp(self):
        self._inventory_apply_env = mock.patch.dict(
            os.environ,
            {"OIAT_ALLOW_INVENTORY_APPLY": "true"},
            clear=False,
        )
        self._inventory_apply_env.start()
        self.addCleanup(self._inventory_apply_env.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _run_apply(self, argv, *, qbo_rows=None, epos_rows=None, post_side_effect=None):
        out_path = self.tmp / "report.csv"
        full_argv = list(argv) + ["--output", str(out_path)]
        patches = _patch_main_dependencies(
            qbo_rows=qbo_rows or _QBO_TROPHY_ROWS,
            epos_rows=epos_rows or _EPOS_TROPHY_ROWS,
            adjust_account_id="82",
        )
        for p in patches:
            p.start()
        post_calls: list[dict] = []

        def fake_post(_tm, _realm, payload):
            post_calls.append(payload)
            if post_side_effect is not None:
                return post_side_effect(payload)
            return {"InventoryAdjustment": {"DocNumber": "INV-1"}}

        fake_lock_result = mock.Mock(acquired=True, reason="")
        fake_lock = mock.Mock()
        fake_lock.acquire.return_value = fake_lock_result
        fake_lock.release.return_value = None

        try:
            buf, err = io.StringIO(), io.StringIO()
            with mock.patch.object(consolidation, "post_inventory_adjustment", side_effect=fake_post), \
                 mock.patch.object(consolidation, "TokenManager"), \
                 mock.patch.object(consolidation, "GlobalRunLock", return_value=fake_lock), \
                 mock.patch.object(consolidation, "mark_qbo_snapshot_stale") as stale_mock, \
                 redirect_stdout(buf), redirect_stderr(err):
                rc = consolidation.main(full_argv)
            return rc, buf.getvalue(), err.getvalue(), post_calls, stale_mock
        finally:
            for p in reversed(patches):
                p.stop()

    def test_apply_posts_trophy_and_marks_snapshot_stale(self):
        rc, _out, _err, posts, stale_mock = self._run_apply([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "1", "--product", "TROPHY",
            "--txn-date", "2026-04-27",
        ])
        self.assertEqual(rc, 0)
        self.assertEqual(len(posts), 1)
        # Lines match TROPHY expected payload
        line_pairs = [
            (l["ItemAdjustmentLineDetail"]["ItemRef"]["value"],
             l["ItemAdjustmentLineDetail"]["QtyDiff"])
            for l in posts[0]["Line"]
        ]
        self.assertEqual(line_pairs, [
            ("9364", 631.0),
            ("9365", -3.0),
            ("9366", -52.0),
        ])
        self.assertEqual(posts[0]["AdjustAccountRef"]["value"], "82")
        self.assertEqual(posts[0]["TxnDate"], "2026-04-27")
        # QBO requires a non-null DocNumber; we generate a deterministic one.
        self.assertEqual(posts[0]["DocNumber"], "INVCON-20260427-9364")
        # Snapshot stale must be marked once with the right reason
        stale_mock.assert_called_once()
        all_args = list(stale_mock.call_args.args) + list(stale_mock.call_args.kwargs.values())
        self.assertIn("company_a", all_args)
        self.assertIn("pack_variant_consolidation_applied", all_args)

    def test_apply_skips_rows_over_max_abs_base_diff(self):
        # Use the default --max-abs-base-diff 1000; +631 fits, but 2000 will be blocked.
        rc, _out, _err, posts, stale_mock = self._run_apply([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "5", "--product", "TROPHY",
            "--max-abs-base-diff", "100",  # forces TROPHY (diff 631) to be blocked
        ])
        self.assertEqual(rc, 0)
        self.assertEqual(posts, [])
        # No successful posts -> snapshot must NOT be marked stale
        stale_mock.assert_not_called()

    def test_apply_skips_rows_over_max_lines(self):
        # Build a scenario with 1 base + many pack variants.
        many_packs = [
            {"Id": "1000", "Name": "P", "base_name": "P", "qbo_has_pack": False, "qbo_qty_on_hand": 0},
        ]
        for i in range(15):
            many_packs.append({
                "Id": f"200{i}", "Name": f"P*{i+2}", "base_name": "P",
                "qbo_has_pack": True, "qbo_qty_on_hand": 1,
            })
        epos = [{"base_name": "P", "epos_single_units": 100, "epos_categories": "X"}]
        rc, _out, _err, posts, stale_mock = self._run_apply([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "5", "--product", "P",
            "--max-lines", "5",
        ], qbo_rows=many_packs, epos_rows=epos)
        self.assertEqual(rc, 0)
        self.assertEqual(posts, [])
        stale_mock.assert_not_called()

    def test_apply_only_posts_consolidation_plan_available(self):
        # Add a needs_manual_review row alongside TROPHY (a base with no
        # variants doesn't generate a plan row, so use a pack-only row).
        qbo = list(_QBO_TROPHY_ROWS) + [
            # Pack variant with no exact base -> needs_manual_review
            {"Id": "7000", "Name": "GHOST*6", "base_name": "GHOST",
             "qbo_has_pack": True, "qbo_qty_on_hand": 5},
        ]
        epos = list(_EPOS_TROPHY_ROWS) + [
            {"base_name": "GHOST", "epos_single_units": 30, "epos_categories": "ALCOHOLS & SPIRITS"},
        ]
        # Scope by category so both rows are in scope for filtering, but the
        # GHOST one is needs_manual_review and must not be posted.
        rc, _out, _err, posts, _stale = self._run_apply([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "5",
            "--category", "ALCOHOLS & SPIRITS",
        ], qbo_rows=qbo, epos_rows=epos)
        self.assertEqual(rc, 0)
        # Exactly one POST — the TROPHY consolidation. GHOST is needs_manual_review
        # and is excluded by _classify_for_apply.
        self.assertEqual(len(posts), 1)
        line_ids = sorted(
            l["ItemAdjustmentLineDetail"]["ItemRef"]["value"] for l in posts[0]["Line"]
        )
        self.assertEqual(line_ids, ["9364", "9365", "9366"])
        # GHOST item should NOT appear in any posted payload
        for payload in posts:
            for line in payload["Line"]:
                self.assertNotEqual(
                    line["ItemAdjustmentLineDetail"]["ItemRef"]["value"], "7000"
                )

    def test_duplicate_doc_number_emits_friendly_message_and_does_not_mark_stale(self):
        # Simulate QBO's duplicate-DocNumber rejection. The CLI should:
        #   - count the row as failed (not silently treat as success)
        #   - print [DUPLICATE] line with the DocNumber and date hint
        #   - NOT mark the snapshot stale (no successful posts)
        from code_scripts.qbo_inventory_adjustment import (
            post_inventory_adjustment as _real_post,  # noqa: F401 import for pattern parity
        )

        def fake_post(payload):
            raise RuntimeError(
                "InventoryAdjustment failed: HTTP 400: "
                "{'Fault': {'Error': [{'Message': 'Duplicate Document Number "
                "Error', 'code': '6240'}], 'type': 'ValidationFault'}}"
            )

        rc, out, err, posts, stale_mock = self._run_apply([
            "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
            "--apply", "--max-products", "1", "--product", "TROPHY",
            "--txn-date", "2026-04-27",
        ], post_side_effect=fake_post)

        # Failure rc, friendly message, no stale mark.
        self.assertEqual(rc, 1)
        self.assertIn("[DUPLICATE]", err)
        self.assertIn("INVCON-20260427-9364", err)
        self.assertIn("may have already been applied", err)
        stale_mock.assert_not_called()
        # Must NOT print the generic [FAIL] header for this specific case.
        self.assertNotIn(
            "[FAIL] base='TROPHY LAGER CAN 500ML' item_id=9364:",
            err,
        )

    def test_apply_does_not_inactivate_any_items(self):
        # The cleanup module exposes _post_inactivate / _fetch_item_with_sync_token.
        # Consolidation must NOT touch those.
        from code_scripts import qbo_pack_variant_cleanup as cleanup
        with mock.patch.object(cleanup, "_post_inactivate") as inact_mock, \
             mock.patch.object(cleanup, "_fetch_item_with_sync_token") as fetch_mock:
            rc, _out, _err, posts, _stale = self._run_apply([
                "--company", "company_a", "--stock-csv", "x", "--qbo-csv", "x",
                "--apply", "--max-products", "1", "--product", "TROPHY",
            ])
        self.assertEqual(rc, 0)
        self.assertEqual(len(posts), 1)
        inact_mock.assert_not_called()
        fetch_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
