import csv
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest import mock

from code_scripts import qbo_pack_variant_cleanup as cleanup
from code_scripts.qbo_pack_variant_cleanup import (
    audit_pack_variants,
    build_inactivate_payload,
    write_report,
    _build_active_base_index,
    _filter_by_product,
)
from code_scripts.transform import strip_pack_multiplier


# ---------------------------------------------------------------------------
# strip_pack_multiplier sanity (the helper this whole tool relies on)
# ---------------------------------------------------------------------------


class StripPackMultiplierTest(unittest.TestCase):
    """Verify the shared helper handles every variant form we'll encounter."""

    def test_strips_no_whitespace(self):
        self.assertEqual(strip_pack_multiplier("PRODUCT*6"), ("PRODUCT", 6))

    def test_strips_left_whitespace(self):
        self.assertEqual(strip_pack_multiplier("PRODUCT *6"), ("PRODUCT", 6))

    def test_strips_both_sides_whitespace(self):
        self.assertEqual(strip_pack_multiplier("PRODUCT * 6"), ("PRODUCT", 6))

    def test_no_pack_returns_multiplier_one(self):
        self.assertEqual(strip_pack_multiplier("PRODUCT"), ("PRODUCT", 1))

    def test_dangling_star_is_not_a_pack(self):
        # No digit after `*` -> not a pack multiplier
        self.assertEqual(strip_pack_multiplier("PRODUCT*"), ("PRODUCT*", 1))

    def test_two_digit_multiplier(self):
        self.assertEqual(strip_pack_multiplier("AQUAFINA 50CL*12"), ("AQUAFINA 50CL", 12))


# ---------------------------------------------------------------------------
# audit_pack_variants classification
# ---------------------------------------------------------------------------


def _row(item_id, name, qty=0):
    return {"Id": str(item_id), "Name": name, "Type": "Inventory", "QtyOnHand": qty}


class AuditClassificationTest(unittest.TestCase):
    def test_safe_to_inactivate_when_exact_base_exists_and_qty_is_zero(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "WIDGET 330ml*12", qty=0),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r["pack_variant_item_id"], "2")
        self.assertEqual(r["base_qbo_item_id"], "1")
        self.assertEqual(r["base_qbo_name"], "WIDGET 330ml")
        self.assertEqual(r["recommended_action"], "safe_to_inactivate_pack_variant")
        self.assertEqual(r["risk_reason"], "")
        self.assertTrue(r["apply_eligible"])
        self.assertEqual(r["apply_block_reason"], "")
        self.assertEqual(r["pack_variant_qty_on_hand"], 0.0)

    def test_needs_manual_review_when_pack_qty_is_nonzero(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "WIDGET 330ml*12", qty=5),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        self.assertEqual(records[0]["recommended_action"], "needs_manual_review")
        self.assertEqual(records[0]["risk_reason"], "pack_variant_has_nonzero_qty_on_hand")
        self.assertFalse(records[0]["apply_eligible"])
        self.assertEqual(
            records[0]["apply_block_reason"], "pack_variant_has_nonzero_qty_on_hand"
        )

    def test_needs_manual_review_when_no_exact_base_in_qbo(self):
        rows = [
            _row(2, "WIDGET 330ml*12", qty=0),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        self.assertEqual(records[0]["recommended_action"], "needs_manual_review")
        self.assertEqual(records[0]["risk_reason"], "no_active_exact_base_in_qbo")
        self.assertEqual(records[0]["base_qbo_item_id"], "")
        self.assertFalse(records[0]["apply_eligible"])

    def test_needs_manual_review_when_multiple_exact_base_in_qbo(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(99, "WIDGET 330ml", qty=2),  # duplicate base — operator data quality issue
            _row(2, "WIDGET 330ml*12", qty=0),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        self.assertEqual(len(records), 1)  # only the *12 row is a pack variant
        self.assertEqual(records[0]["recommended_action"], "needs_manual_review")
        self.assertEqual(
            records[0]["risk_reason"], "multiple_active_exact_base_in_qbo"
        )
        self.assertFalse(records[0]["apply_eligible"])

    def test_non_pack_variants_are_skipped_entirely(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=10),
            _row(2, "ANOTHER PRODUCT", qty=5),
        ]
        self.assertEqual(audit_pack_variants(rows, company_key="company_a"), [])

    def test_base_index_is_case_insensitive(self):
        rows = [
            _row(1, "Widget 330ml", qty=10),
            _row(2, "WIDGET 330ML*12", qty=0),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        self.assertEqual(records[0]["recommended_action"], "safe_to_inactivate_pack_variant")
        self.assertEqual(records[0]["base_qbo_item_id"], "1")


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


class FilterTest(unittest.TestCase):
    def setUp(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=0),
            _row(2, "WIDGET 330ml*12", qty=0),
            _row(3, "GADGET 1L", qty=0),
            _row(4, "GADGET 1L*6", qty=0),
        ]
        self.records = audit_pack_variants(rows, company_key="company_a")

    def test_product_filter_substring(self):
        narrowed = _filter_by_product(self.records, "GADGET")
        self.assertEqual(len(narrowed), 1)
        self.assertEqual(narrowed[0]["pack_variant_name"], "GADGET 1L*6")

    def test_product_filter_empty_keeps_all(self):
        self.assertEqual(_filter_by_product(self.records, ""), self.records)


# ---------------------------------------------------------------------------
# Payload + report
# ---------------------------------------------------------------------------


class PayloadTest(unittest.TestCase):
    def test_inactivate_payload_shape(self):
        payload = build_inactivate_payload(
            item_id="555",
            sync_token="3",
            original_name="WIDGET 330ml*12",
        )
        self.assertEqual(payload["Id"], "555")
        self.assertEqual(payload["SyncToken"], "3")
        self.assertTrue(payload["sparse"])
        self.assertEqual(payload["Name"], "WIDGET 330ml*12 (old-555)")
        self.assertFalse(payload["Active"])


class WriteReportTest(unittest.TestCase):
    def test_report_has_expected_columns(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=0),
            _row(2, "WIDGET 330ml*12", qty=0),
        ]
        records = audit_pack_variants(rows, company_key="company_a")
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "report.csv"
            write_report(records, out_path)
            with open(out_path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames
                rows_out = list(reader)
        for required in (
            "company_key",
            "base_name",
            "base_qbo_item_id",
            "base_qbo_name",
            "pack_variant_item_id",
            "pack_variant_name",
            "pack_variant_qty_on_hand",
            "recommended_action",
            "risk_reason",
            "apply_eligible",
            "apply_block_reason",
            "pack_variant_sync_token",
            "pack_variant_active",
            "base_qbo_active",
        ):
            self.assertIn(required, fields)
        self.assertEqual(rows_out[0]["recommended_action"], "safe_to_inactivate_pack_variant")
        self.assertEqual(rows_out[0]["apply_eligible"], "True")


# ---------------------------------------------------------------------------
# main(): apply-mode guards (dry-run-no-call, --max-items required, etc.)
# ---------------------------------------------------------------------------


class _FakeConfig:
    company_key = "company_a"
    realm_id = "REALM"
    display_name = "Company A"
    slack_webhook_url = None


def _stub_load_qbo_inventory_item_rows(rows: list[dict]) -> "object":
    """Build a stubbed pandas DataFrame-like object that yields ``rows`` from
    ``.to_dict(orient='records')``. Avoids importing pandas in tests."""

    class _Stub:
        def to_dict(self, orient: str = "records"):  # noqa: D401
            return list(rows)

    return _Stub()


def _common_main_patches(rows):
    """Yield context managers that stub out config + QBO snapshot load."""
    # `_resolve_qbo_csv` shells out to either fetch_qbo_inventory_items_snapshot
    # or to a path on disk; easier to bypass entirely with a fixed Path return.
    return [
        mock.patch.object(cleanup, "load_company_config", return_value=_FakeConfig()),
        mock.patch.object(cleanup, "ensure_company_runtime_compatible"),
        mock.patch.object(
            cleanup, "get_available_companies", return_value=["company_a"]
        ),
        mock.patch.object(cleanup, "_resolve_qbo_csv", return_value=Path("/dev/null")),
        mock.patch.object(
            cleanup,
            "load_qbo_inventory_item_rows",
            return_value=_stub_load_qbo_inventory_item_rows(rows),
        ),
        mock.patch.object(cleanup, "verify_realm_match"),
    ]


class ApplyModeGuardsTest(unittest.TestCase):
    def setUp(self):
        self._inventory_apply_env = mock.patch.dict(
            os.environ,
            {"OIAT_ALLOW_INVENTORY_APPLY": "true"},
            clear=False,
        )
        self._inventory_apply_env.start()
        self.addCleanup(self._inventory_apply_env.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, argv, qbo_rows):
        out_path = self.tmp_path / "report.csv"
        full_argv = argv + ["--output", str(out_path)]
        patches = _common_main_patches(qbo_rows)
        for p in patches:
            p.start()
        try:
            buf = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(buf), redirect_stderr(err):
                rc = cleanup.main(full_argv)
            return rc, buf.getvalue(), err.getvalue(), out_path
        finally:
            for p in reversed(patches):
                p.stop()

    def test_apply_without_max_items_returns_2(self):
        rc, _out, err, _ = self._run(
            ["--company", "company_a", "--qbo-csv", "x", "--apply"],
            qbo_rows=[],
        )
        self.assertEqual(rc, 2)
        self.assertIn("--apply requires --max-items", err)

    def test_apply_with_zero_max_items_returns_2(self):
        rc, _out, err, _ = self._run(
            ["--company", "company_a", "--qbo-csv", "x", "--apply", "--max-items", "0"],
            qbo_rows=[],
        )
        self.assertEqual(rc, 2)
        self.assertIn("--max-items must be > 0", err)

    def test_apply_and_dry_run_together_returns_2(self):
        rc, _out, err, _ = self._run(
            ["--company", "company_a", "--qbo-csv", "x", "--apply", "--max-items", "1", "--dry-run"],
            qbo_rows=[],
        )
        self.assertEqual(rc, 2)
        self.assertIn("--apply or --dry-run", err)

    def test_dry_run_does_not_call_qbo_update(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=0),
            _row(2, "WIDGET 330ml*12", qty=0),
        ]
        with mock.patch.object(cleanup, "_post_inactivate") as post_mock, \
             mock.patch.object(cleanup, "_fetch_item_with_sync_token") as fetch_mock:
            rc, out, _err, _path = self._run(
                ["--company", "company_a", "--qbo-csv", "x", "--dry-run"],
                qbo_rows=rows,
            )
        self.assertEqual(rc, 0)
        post_mock.assert_not_called()
        fetch_mock.assert_not_called()
        self.assertIn("[APPLY-PLAN]", out)
        self.assertIn("WIDGET 330ml*12", out)

    def test_apply_only_touches_safe_rows_and_marks_snapshot_stale(self):
        rows = [
            _row(1, "WIDGET 330ml", qty=0),
            _row(2, "WIDGET 330ml*12", qty=0),  # safe (qty 0, exact base exists)
            _row(3, "GADGET 1L*6", qty=0),       # needs_manual_review (no base)
            _row(4, "FOO 1L", qty=0),
            _row(5, "FOO 1L*6", qty=4),          # needs_manual_review (qty != 0)
        ]
        post_calls: list[dict] = []

        def fake_fetch(_tm, _realm, item_id):
            return {"Id": str(item_id), "SyncToken": "0"}

        def fake_post(_tm, _realm, payload):
            post_calls.append(payload)
            return {"Item": {"Id": payload["Id"]}}

        # Patch the GlobalRunLock to acquire successfully without touching disk.
        fake_lock_result = mock.Mock(acquired=True, reason="")
        fake_lock = mock.Mock()
        fake_lock.acquire.return_value = fake_lock_result
        fake_lock.release.return_value = None

        with mock.patch.object(cleanup, "_fetch_item_with_sync_token", side_effect=fake_fetch), \
             mock.patch.object(cleanup, "_post_inactivate", side_effect=fake_post), \
             mock.patch.object(cleanup, "GlobalRunLock", return_value=fake_lock), \
             mock.patch.object(cleanup, "TokenManager"), \
             mock.patch.object(cleanup, "mark_qbo_snapshot_stale") as stale_mock:
            rc, out, _err, _path = self._run(
                ["--company", "company_a", "--qbo-csv", "x", "--apply", "--max-items", "5"],
                qbo_rows=rows,
            )

        self.assertEqual(rc, 0)
        # Only the safe row (item id=2) should be inactivated.
        self.assertEqual(len(post_calls), 1)
        self.assertEqual(post_calls[0]["Id"], "2")
        self.assertEqual(post_calls[0]["Active"], False)
        self.assertEqual(post_calls[0]["Name"], "WIDGET 330ml*12 (old-2)")
        self.assertTrue(post_calls[0]["sparse"])
        # Snapshot cache must be invalidated after a successful apply.
        stale_mock.assert_called_once()
        kwargs = stale_mock.call_args.kwargs
        args = stale_mock.call_args.args
        all_args = list(args) + list(kwargs.values())
        self.assertIn("company_a", all_args)
        self.assertIn("pack_variant_cleanup_applied", all_args)
        # Apply summary line emitted.
        self.assertIn("Apply summary: attempted=1 succeeded=1", out)

    def test_apply_max_items_caps_count(self):
        # Build 3 safe rows; cap to 2.
        rows = []
        for n in range(3):
            rows.append(_row(100 + n, f"P{n}", qty=0))
            rows.append(_row(200 + n, f"P{n}*12", qty=0))
        post_calls: list[dict] = []

        fake_lock_result = mock.Mock(acquired=True, reason="")
        fake_lock = mock.Mock()
        fake_lock.acquire.return_value = fake_lock_result

        with mock.patch.object(cleanup, "_fetch_item_with_sync_token",
                               side_effect=lambda *_a, **_k: {"SyncToken": "0"}), \
             mock.patch.object(cleanup, "_post_inactivate",
                               side_effect=lambda _tm, _realm, payload: post_calls.append(payload) or {"Item": {"Id": payload["Id"]}}), \
             mock.patch.object(cleanup, "GlobalRunLock", return_value=fake_lock), \
             mock.patch.object(cleanup, "TokenManager"), \
             mock.patch.object(cleanup, "mark_qbo_snapshot_stale"):
            rc, out, _err, _path = self._run(
                ["--company", "company_a", "--qbo-csv", "x", "--apply", "--max-items", "2"],
                qbo_rows=rows,
            )

        self.assertEqual(rc, 0)
        self.assertEqual(len(post_calls), 2)
        self.assertIn("succeeded=2", out)
        self.assertIn("skipped_due_to_cap=1", out)


if __name__ == "__main__":
    unittest.main()
