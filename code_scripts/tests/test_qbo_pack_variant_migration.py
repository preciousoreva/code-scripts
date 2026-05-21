import csv
import tempfile
import unittest
from pathlib import Path

from code_scripts.qbo_pack_variant_cleanup import audit_pack_variants
from code_scripts.qbo_pack_variant_migration import (
    build_migration_plan,
    write_report,
    _REPORT_FIELDS,
)


def _row(item_id, name, qty=0):
    return {"Id": str(item_id), "Name": name, "Type": "Inventory", "QtyOnHand": qty}


def _audit(rows):
    return audit_pack_variants(rows, company_key="company_a")


# ---------------------------------------------------------------------------
# Multiplier math
# ---------------------------------------------------------------------------


class MultiplierMathTest(unittest.TestCase):
    def test_pack12_qty5_yields_base_delta_60_pack_delta_neg5(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),  # active base
            _row(2, "WIDGET 330ml*12", qty=5),  # pack variant w/ qty
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["multiplier"], 12)
        self.assertEqual(r["pack_variant_qty_on_hand"], 5.0)
        self.assertEqual(r["proposed_base_qty_delta"], 60.0)
        self.assertEqual(r["proposed_pack_variant_qty_delta"], -5.0)
        self.assertEqual(r["migration_recommended_action"], "migration_plan_available")
        self.assertEqual(r["risk_reason"], "")
        self.assertEqual(r["base_qbo_item_id"], "1")
        self.assertEqual(r["base_qbo_name"], "WIDGET 330ml")

    def test_pack24_qty2_yields_base_delta_48(self):
        rows = [
            _row(1, "JUICE 250ml", qty=0),
            _row(2, "JUICE 250ml*24", qty=2),
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(plan[0]["multiplier"], 24)
        self.assertEqual(plan[0]["proposed_base_qty_delta"], 48.0)
        self.assertEqual(plan[0]["proposed_pack_variant_qty_delta"], -2.0)

    def test_negative_pack_qty_handled(self):
        # QBO occasionally shows negative qty (oversold). Math is the same.
        rows = [
            _row(1, "BEER CAN 50cl", qty=0),
            _row(2, "BEER CAN 50cl*6", qty=-3),
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(plan[0]["multiplier"], 6)
        self.assertEqual(plan[0]["proposed_base_qty_delta"], -18.0)
        self.assertEqual(plan[0]["proposed_pack_variant_qty_delta"], 3.0)
        self.assertEqual(plan[0]["migration_recommended_action"], "migration_plan_available")

    def test_pack_qty_zero_rows_are_excluded_from_plan(self):
        # Pack qty 0 is the cleanup tool's territory; nothing to migrate.
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "WIDGET 330ml*12", qty=0),  # cleanup-safe; should not appear
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(plan, [])

    def test_non_pack_items_are_excluded(self):
        # Items without a *N suffix never enter the plan.
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "ANOTHER PRODUCT", qty=8),
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(plan, [])


# ---------------------------------------------------------------------------
# needs_manual_review cases
# ---------------------------------------------------------------------------


class ManualReviewTest(unittest.TestCase):
    def test_no_active_exact_base_yields_manual_review(self):
        rows = [
            _row(2, "WIDGET 330ml*12", qty=5),  # no base in catalog
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["migration_recommended_action"], "needs_manual_review")
        self.assertEqual(r["risk_reason"], "no_active_exact_base_in_qbo")
        self.assertEqual(r["proposed_base_qty_delta"], "")
        self.assertEqual(r["proposed_pack_variant_qty_delta"], "")
        # multiplier is still useful diagnostic info even when no base
        self.assertEqual(r["multiplier"], 12)
        self.assertEqual(r["pack_variant_qty_on_hand"], 5.0)
        self.assertEqual(r["base_qbo_item_id"], "")

    def test_multiple_active_exact_base_yields_manual_review(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=5),
            _row(99, "WIDGET 330ml", qty=2),  # duplicate active base
            _row(2, "WIDGET 330ml*12", qty=5),
        ]
        plan = build_migration_plan(_audit(rows))
        self.assertEqual(len(plan), 1)
        r = plan[0]
        self.assertEqual(r["migration_recommended_action"], "needs_manual_review")
        self.assertEqual(r["risk_reason"], "multiple_active_exact_base_in_qbo")
        self.assertEqual(r["proposed_base_qty_delta"], "")
        self.assertEqual(r["proposed_pack_variant_qty_delta"], "")


# ---------------------------------------------------------------------------
# Mixed scenarios
# ---------------------------------------------------------------------------


class MixedScenarioTest(unittest.TestCase):
    def test_mix_of_actions_in_one_run(self):
        rows = [
            # Eligible: has base, pack qty nonzero
            _row(1, "ALPHA", qty=20),
            _row(2, "ALPHA*6", qty=3),
            # Manual review: no base, pack qty nonzero
            _row(3, "BETA*12", qty=4),
            # Manual review: multiple bases, pack qty nonzero
            _row(4, "GAMMA", qty=1),
            _row(5, "GAMMA", qty=2),
            _row(6, "GAMMA*24", qty=1),
            # Excluded: pack qty 0 (cleanup territory)
            _row(7, "DELTA", qty=0),
            _row(8, "DELTA*6", qty=0),
            # Excluded: not a pack variant at all
            _row(9, "EPSILON", qty=99),
        ]
        plan = build_migration_plan(_audit(rows))
        actions = sorted(r["migration_recommended_action"] for r in plan)
        self.assertEqual(actions, ["migration_plan_available", "needs_manual_review", "needs_manual_review"])
        eligible = [r for r in plan if r["migration_recommended_action"] == "migration_plan_available"]
        self.assertEqual(eligible[0]["base_name"], "ALPHA")
        self.assertEqual(eligible[0]["proposed_base_qty_delta"], 18.0)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class WriteReportTest(unittest.TestCase):
    def test_report_has_expected_columns(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "WIDGET 330ml*12", qty=5),
        ]
        plan = build_migration_plan(_audit(rows))
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "plan.csv"
            write_report(plan, out_path)
            with open(out_path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames
                rows_out = list(reader)

        for required in _REPORT_FIELDS:
            self.assertIn(required, fields)
        self.assertEqual(rows_out[0]["multiplier"], "12")
        self.assertEqual(rows_out[0]["proposed_base_qty_delta"], "60.0")
        self.assertEqual(rows_out[0]["proposed_pack_variant_qty_delta"], "-5.0")
        self.assertEqual(rows_out[0]["migration_recommended_action"], "migration_plan_available")
        self.assertEqual(rows_out[0]["risk_reason"], "")


if __name__ == "__main__":
    unittest.main()
