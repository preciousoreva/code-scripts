import csv
import tempfile
import unittest
from pathlib import Path

from code_scripts.qbo_pack_variant_consolidation import (
    build_consolidation_plan,
    write_report,
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


if __name__ == "__main__":
    unittest.main()
