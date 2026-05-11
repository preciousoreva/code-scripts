import io
import os
import unittest
from contextlib import redirect_stdout
from unittest import mock

import pandas as pd

from code_scripts import inventory_sync, qbo_snapshot_cache
from code_scripts.inventory_sync import choose_canonical_qbo_item_row
from code_scripts.qbo_inventory_adjustment import build_inventory_adjustment_payload


class InventorySyncHelpersTest(unittest.TestCase):
    def test_build_inventory_adjustment_doc_number(self):
        self.assertEqual(
            inventory_sync.build_inventory_adjustment_doc_number("2026-04-28", 9124),
            "INVADJ-20260428-9124",
        )
        self.assertEqual(
            inventory_sync.build_inventory_adjustment_doc_number("2026-04-28", "9124"),
            "INVADJ-20260428-9124",
        )

    def test_choose_canonical_prefers_exact_base_name(self):
        rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "WIDGET 330ml*12", "qbo_has_pack": True, "qbo_qty_on_hand": 100.0},
                {"Id": "11", "Name": "WIDGET 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": 3.0},
            ]
        )
        chosen, reason = choose_canonical_qbo_item_row(rows, base_name="WIDGET 330ml")
        self.assertEqual(str(chosen["Id"]), "11")
        self.assertEqual(reason, "exact_name_match")

    def test_build_inventory_adjustment_payload_shape(self):
        payload = build_inventory_adjustment_payload(
            adjust_account_id="99",
            txn_date="2026-04-14",
            private_note="test",
            lines=[{"item_id": "123", "qty_diff": -2.5}],
        )
        self.assertEqual(payload["TxnDate"], "2026-04-14")
        self.assertEqual(payload["AdjustAccountRef"]["value"], "99")
        self.assertEqual(len(payload["Line"]), 1)
        line0 = payload["Line"][0]
        self.assertEqual(line0["DetailType"], "ItemAdjustmentLineDetail")
        self.assertEqual(line0["ItemAdjustmentLineDetail"]["ItemRef"]["value"], "123")
        self.assertEqual(line0["ItemAdjustmentLineDetail"]["QtyDiff"], -2.5)

    def test_load_epos_stock_snapshot_filters_to_selected_categories(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            stock_csv = Path(td) / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "WIDGET 330ml,ALCOHOLS & SPIRITS,5\n"
                "WIDGET 330ml*12,ALCOHOLS & SPIRITS,1\n"
                "SOAP,HOUSEHOLD,4\n",
                encoding="utf-8",
            )
            grouped = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                categories=["alcohols & spirits"],
            )

        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped.iloc[0]["base_name"], "WIDGET 330ml")
        self.assertEqual(grouped.iloc[0]["epos_single_units"], 17.0)
        self.assertEqual(grouped.iloc[0]["epos_categories"], "ALCOHOLS & SPIRITS")
        self.assertEqual(grouped.iloc[0]["epos_category_count"], 1)

    def test_load_epos_stock_snapshot_base_name_filter_exact_and_includes_pack_variants(self):
        """base_names filtering must match exact normalized base_name_norm (not substring),
        and include pack variants that normalize to the same base."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            stock_csv = Path(td) / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "CHEESE BALLS 13g,Snacks,2\n"
                "CHEESE BALLS 13g*50,Snacks,1\n"
                "CHEESE BALLS90g,Snacks,9\n"
                "SOME OTHER ITEM,General,3\n",
                encoding="utf-8",
            )
            grouped = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                base_names=["CHEESE BALLS 13g"],
            )

        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped.iloc[0]["base_name"], "CHEESE BALLS 13g")
        # Includes pack row (multiplier 50) + base row.
        # base 2 + (pack 1 * 50) = 52
        self.assertEqual(grouped.iloc[0]["epos_single_units"], 52.0)
        # Substring-like near-miss must be excluded.
        self.assertNotIn("CHEESE BALLS90g", grouped["base_name"].tolist())

    def test_load_epos_stock_snapshot_base_name_filter_normalizes_spaces_and_case(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            stock_csv = Path(td) / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "CHEESE BALLS 13g,Snacks,2\n"
                "CHEESE BALLS 13g*50,Snacks,1\n",
                encoding="utf-8",
            )
            grouped = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                base_names=["  cheese   balls 13G  "],
            )

        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped.iloc[0]["base_name"], "CHEESE BALLS 13g")

    def test_load_epos_stock_snapshot_base_name_filter_empty_means_no_filter(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            stock_csv = Path(td) / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "CHEESE BALLS 13g,Snacks,2\n"
                "CHEESE BALLS90g,Snacks,9\n",
                encoding="utf-8",
            )
            grouped_none = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                base_names=None,
            )
            grouped_empty = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                base_names=[],
            )

        self.assertEqual(set(grouped_none["base_name"].tolist()), {"CHEESE BALLS 13g", "CHEESE BALLS90g"})
        self.assertEqual(set(grouped_empty["base_name"].tolist()), {"CHEESE BALLS 13g", "CHEESE BALLS90g"})

    def test_load_epos_stock_snapshot_base_name_filter_is_not_substring_match(self):
        """Selecting a partial base name must not include other bases that merely contain it."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            stock_csv = Path(td) / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "CHEESE BALLS 13g,Snacks,2\n"
                "CHEESE BALLS 13g*50,Snacks,1\n"
                "CHEESE BALLS90g,Snacks,9\n",
                encoding="utf-8",
            )
            grouped = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                base_names=["CHEESE BALLS"],
            )

        # There is no exact base_name_norm of "CHEESE BALLS" in the input, so nothing should match.
        self.assertEqual(len(grouped), 0)


class QboSnapshotBaseNameFilterTest(unittest.TestCase):
    def _write_qbo_csv(self, path):
        # Minimal columns used by load_qbo_inventory_snapshot.
        path.write_text(
            "Id,Name,Type,TrackQtyOnHand,QtyOnHand,Active\n"
            "1,CHEESE BALLS 13g,Inventory,true,5,true\n"
            "2,CHEESE BALLS 13g*50,Inventory,true,7,true\n"
            "3,CHEESE BALLS90g,Inventory,true,9,true\n"
            "4,SOME OTHER ITEM,Inventory,true,3,true\n",
            encoding="utf-8",
        )

    def test_load_qbo_inventory_snapshot_base_name_filter_exact_and_includes_pack_variants(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            qbo_csv = Path(td) / "qbo.csv"
            self._write_qbo_csv(qbo_csv)
            grouped = inventory_sync.load_qbo_inventory_snapshot(
                str(qbo_csv),
                base_names=["CHEESE BALLS 13g"],
            )

        self.assertEqual(len(grouped), 1)
        row = grouped.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "CHEESE BALLS 13g")
        # Pack variants for the base remain included in grouped naming fields.
        self.assertIn("CHEESE BALLS 13g", str(row.get("qbo_item_names_for_base") or ""))
        self.assertIn("CHEESE BALLS 13g*50", str(row.get("qbo_item_names_for_base") or ""))
        self.assertTrue(bool(row.get("qbo_has_pack_variants")))
        self.assertGreater(int(row.get("qbo_active_pack_variant_count") or 0), 0)

    def test_load_qbo_inventory_snapshot_base_name_filter_normalizes_spaces_and_case(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            qbo_csv = Path(td) / "qbo.csv"
            self._write_qbo_csv(qbo_csv)
            grouped = inventory_sync.load_qbo_inventory_snapshot(
                str(qbo_csv),
                base_names=["  cheese   balls 13G  "],
            )

        self.assertEqual(len(grouped), 1)
        row = grouped.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "CHEESE BALLS 13g")
        self.assertIn("CHEESE BALLS 13g*50", str(row.get("qbo_item_names_for_base") or ""))

    def test_load_qbo_inventory_snapshot_base_name_filter_empty_means_no_filter(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            qbo_csv = Path(td) / "qbo.csv"
            self._write_qbo_csv(qbo_csv)
            grouped_none = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv), base_names=None)
            grouped_empty = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv), base_names=[])

        # Grouped snapshot is per-base, so CHEESE BALLS 13g and CHEESE BALLS 13g*50 collapse to one base.
        self.assertEqual(len(grouped_none), 3)
        self.assertEqual(len(grouped_empty), 3)

    def test_load_qbo_inventory_snapshot_base_name_filter_is_not_substring_match(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            qbo_csv = Path(td) / "qbo.csv"
            self._write_qbo_csv(qbo_csv)
            grouped = inventory_sync.load_qbo_inventory_snapshot(
                str(qbo_csv),
                base_names=["CHEESE BALLS"],
            )

        # No exact base_name_norm of "CHEESE BALLS" exists, so nothing should match.
        self.assertEqual(len(grouped), 0)


class InventorySyncRuntimeGuardTest(unittest.TestCase):
    """main() must refuse to continue when OIAT_RUNTIME_ENV does not match
    the loaded company config's qbo.environment."""

    def test_sandbox_runtime_with_production_config_exits_early(self):
        fake_config = mock.Mock()
        fake_config.company_key = "company_a"
        fake_config.display_name = "ACME"
        fake_config.qbo_environment = "production"

        buf = io.StringIO()
        with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "sandbox"}), \
             mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
             mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
             redirect_stdout(buf):
            exit_code = inventory_sync.main([
                "--company", "company_a",
                "--stock-csv", "/nonexistent/stock.csv",
            ])
        self.assertEqual(exit_code, 2)
        self.assertIn("QBO environment mismatch", buf.getvalue())


class InventoryMaxQtyDeltaConfigTest(unittest.TestCase):
    def test_env_override_wins_over_config(self):
        from code_scripts.company_config import CompanyConfig

        data = {
            "company_key": "company_a",
            "display_name": "ACME",
            "qbo": {"environment": "production", "inventory_max_qty_delta": 100},
        }
        cfg = CompanyConfig.__new__(CompanyConfig)
        cfg._data = data  # type: ignore[attr-defined]
        with mock.patch.dict(os.environ, {"COMPANY_A_INVENTORY_MAX_QTY_DELTA": "42"}, clear=False):
            self.assertEqual(cfg.inventory_max_qty_delta, 42.0)

    def test_missing_returns_none(self):
        from code_scripts.company_config import CompanyConfig

        cfg = CompanyConfig.__new__(CompanyConfig)
        cfg._data = {"company_key": "company_a", "qbo": {}}  # type: ignore[attr-defined]
        env_clean = {k: v for k, v in os.environ.items() if k != "COMPANY_A_INVENTORY_MAX_QTY_DELTA"}
        with mock.patch.dict(os.environ, env_clean, clear=True):
            self.assertIsNone(cfg.inventory_max_qty_delta)


class InventoryAuditMetadataSidecarTest(unittest.TestCase):
    """_write_audit_metadata must drop a parseable sidecar next to the report CSV,
    and include OIAT_RUN_JOB_ID when present in env."""

    def _call(self, *, env_job_id=None):
        import json
        import tempfile
        from pathlib import Path

        from code_scripts.inventory_sync import _write_audit_metadata

        with tempfile.TemporaryDirectory() as td:
            report = Path(td) / "inventory_audit_company_a_2026.csv"
            env = {k: v for k, v in os.environ.items()}
            if env_job_id is None:
                env.pop("OIAT_RUN_JOB_ID", None)
            else:
                env["OIAT_RUN_JOB_ID"] = env_job_id
            with mock.patch.dict(os.environ, env, clear=True):
                meta_path = _write_audit_metadata(
                    report,
                    company_key="company_a",
                    display_name="ACME",
                    stock_csv="/tmp/stock.csv",
                    qbo_csv="/tmp/qbo.csv",
                    status_counts={"in_sync": 5, "needs_adjustment": 1},
                    total_groups=6,
                    apply_stats={"mode": "audit_only", "posted": 0, "skipped": 0},
                )
            self.assertTrue(meta_path.exists())
            return json.loads(meta_path.read_text(encoding="utf-8"))

    def test_writes_sidecar_without_run_job_id(self):
        data = self._call()
        self.assertEqual(data["company_key"], "company_a")
        self.assertEqual(data["display_name"], "ACME")
        self.assertEqual(data["total_groups"], 6)
        self.assertEqual(data["status_counts"]["in_sync"], 5)
        self.assertEqual(data["apply"]["mode"], "audit_only")
        self.assertNotIn("run_job_id", data)

    def test_writes_sidecar_with_run_job_id_from_env(self):
        data = self._call(env_job_id="abc-123")
        self.assertEqual(data["run_job_id"], "abc-123")


class InventorySyncSlackNotificationTest(unittest.TestCase):
    def _build_fake_config(self):
        cfg = mock.Mock()
        cfg.company_key = "company_a"
        cfg.display_name = "ACME"
        cfg.qbo_environment = "production"
        cfg.realm_id = "REALM123"
        cfg.inventory_max_qty_delta = None
        cfg.inventory_adjustment_account_id = ""
        cfg.slack_webhook_url = "https://hooks.slack.test/example"
        return cfg

    def _run_audit(self, extra_args=None, env_extra=None, qbo_qty=5):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        extra_args = list(extra_args or [])
        env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
        env.pop("OIAT_RUN_JOB_ID", None)
        if env_extra:
            env.update(env_extra)

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\nWidget,ALCOHOLS,5\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                f"10,Widget,Inventory,true,{qbo_qty}\n",
                encoding="utf-8",
            )
            report_path = tdp / "report.csv"

            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "send_slack_success") as slack_mock, \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--qbo-csv", str(qbo_csv),
                    "--output", str(report_path),
                    *extra_args,
                ])

        return exit_code, slack_mock

    def test_audit_only_cli_does_not_notify_slack_by_default(self):
        exit_code, slack_mock = self._run_audit()
        self.assertEqual(exit_code, 0)
        slack_mock.assert_not_called()

    def test_audit_only_notify_slack_flag_opts_in(self):
        exit_code, slack_mock = self._run_audit(["--notify-slack"])
        self.assertEqual(exit_code, 0)
        slack_mock.assert_called_once()

    def test_audit_only_job_context_notifies_slack(self):
        exit_code, slack_mock = self._run_audit(env_extra={"OIAT_RUN_JOB_ID": "job-123"})
        self.assertEqual(exit_code, 0)
        slack_mock.assert_called_once()

    def test_dry_run_does_not_notify_slack_by_default(self):
        exit_code, slack_mock = self._run_audit(
            ["--dry-run", "--adjust-account-id", "88"],
            qbo_qty=1,
        )
        self.assertEqual(exit_code, 0)
        slack_mock.assert_not_called()

    def test_audit_slack_examples_include_pack_variant_names_when_only_pack_exists(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        env = {**os.environ, "OIAT_RUNTIME_ENV": "production", "OIAT_RUN_JOB_ID": "job-123"}

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "BACARDI WHITE RUM 750ml,ALCOHOLS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,BACARDI WHITE RUM 750ml*12,Inventory,true,1\n",
                encoding="utf-8",
            )
            report_path = tdp / "report.csv"

            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "send_slack_success") as slack_mock, \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--qbo-csv", str(qbo_csv),
                    "--output", str(report_path),
                ])

        self.assertEqual(exit_code, 0)
        slack_mock.assert_called_once()
        msg = slack_mock.call_args[0][0]
        self.assertIn("BACARDI WHITE RUM 750ml*12", msg)


class InventorySyncAutoDownloadWiringTest(unittest.TestCase):
    """--auto-download wires inventory_sync to the EPOS StockReport Playwright
    downloader without requiring the operator to pre-fetch a CSV.

    The downloader itself is mocked here so these tests don't need Playwright
    installed or a real EPOS session — we only verify the dispatch contract.
    """

    def _build_fake_config(self):
        cfg = mock.Mock()
        cfg.company_key = "company_a"
        cfg.display_name = "ACME"
        cfg.qbo_environment = "production"
        cfg.realm_id = "REALM123"
        cfg.inventory_max_qty_delta = None
        cfg.inventory_adjustment_account_id = ""
        cfg.slack_webhook_url = ""
        return cfg

    def test_main_errors_when_neither_stock_csv_nor_auto_download_given(self):
        fake_config = self._build_fake_config()
        buf = io.StringIO()
        env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
             mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
             redirect_stdout(buf):
            exit_code = inventory_sync.main(["--company", "company_a"])
        self.assertEqual(exit_code, 1)
        self.assertIn("--stock-csv", buf.getvalue())
        self.assertIn("--auto-download", buf.getvalue())

    def test_auto_download_invokes_helper_and_uses_returned_path(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        recorded: dict = {}

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "company_a_StockReport.csv"
            stock_csv.write_text(
                "Name,MeasuredCurrentStock\nWidget 330ml,5\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "company_a_products.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget 330ml,Inventory,true,5\n",
                encoding="utf-8",
            )
            report_path = tdp / "report.csv"

            def fake_download(config, *, output_dir, download_timeout_ms, headful):
                recorded["company_key"] = config.company_key
                recorded["output_dir"] = output_dir
                recorded["download_timeout_ms"] = download_timeout_ms
                recorded["headful"] = headful
                return stock_csv

            buf = io.StringIO()
            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "_auto_download_stock_csv", side_effect=fake_download), \
                 redirect_stdout(buf):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--auto-download",
                    "--download-headful",
                    "--download-timeout-ms", "120000",
                    "--qbo-csv", str(qbo_csv),
                    "--output", str(report_path),
                ])

        self.assertEqual(exit_code, 0)
        self.assertEqual(recorded.get("company_key"), "company_a")
        self.assertEqual(recorded.get("download_timeout_ms"), 120000)
        self.assertTrue(recorded.get("headful"))
        # Console should mention the auto-downloaded path
        self.assertIn("Downloaded stock CSV", buf.getvalue())
        # Audit should have run
        self.assertIn("Inventory audit", buf.getvalue())

    def test_explicit_stock_csv_skips_auto_download_helper(self):
        """Belt-and-suspenders: if both flags are passed, --stock-csv wins
        and the downloader is never invoked."""
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,MeasuredCurrentStock\nWidget,1\n", encoding="utf-8"
            )
            qbo_csv = tdp / "products.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget,Inventory,true,1\n",
                encoding="utf-8",
            )

            mocked_download = mock.Mock(side_effect=AssertionError(
                "_auto_download_stock_csv should not be called when --stock-csv is provided"
            ))

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "_auto_download_stock_csv", mocked_download), \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--auto-download",  # should be ignored when --stock-csv is set
                    "--qbo-csv", str(qbo_csv),
                    "--output", str(tdp / "out.csv"),
                ])

        self.assertEqual(exit_code, 0)
        mocked_download.assert_not_called()


class InventorySyncAutoDownloadHelperTest(unittest.TestCase):
    """_auto_download_stock_csv must call the EPOS Playwright runner with the
    operator-supplied flags and return the saved path as a Path."""

    def test_calls_playwright_runner_with_expected_kwargs(self):
        from pathlib import Path

        from code_scripts import epos_stocklevels_playwright

        fake_config = mock.Mock(company_key="company_sandbox")
        fake_playwright = mock.MagicMock()
        captured: dict = {}

        def fake_run(playwright, config, *, output_dir, download_timeout_ms, headful):
            captured["playwright"] = playwright
            captured["config"] = config
            captured["output_dir"] = output_dir
            captured["download_timeout_ms"] = download_timeout_ms
            captured["headful"] = headful
            return "/tmp/oiat-test/company_sandbox_StockReport_2026-04-23.csv"

        # Patch the attributes the lazy import inside _auto_download_stock_csv
        # actually uses. sync_playwright is a context manager; epos_stocklevels_
        # playwright.run is the function we want to verify the call to.
        sync_pw_cm = mock.MagicMock()
        sync_pw_cm.__enter__.return_value = fake_playwright
        sync_pw_cm.__exit__.return_value = False

        import playwright.sync_api as _pw_sync_api

        with mock.patch.object(_pw_sync_api, "sync_playwright", return_value=sync_pw_cm), \
             mock.patch.object(epos_stocklevels_playwright, "run", side_effect=fake_run):
            saved = inventory_sync._auto_download_stock_csv(
                fake_config,
                output_dir="/tmp/oiat-test",
                download_timeout_ms=42000,
                headful=True,
            )

        self.assertIsInstance(saved, Path)
        self.assertEqual(captured["config"], fake_config)
        self.assertEqual(captured["output_dir"], "/tmp/oiat-test")
        self.assertEqual(captured["download_timeout_ms"], 42000)
        self.assertTrue(captured["headful"])
        self.assertIs(captured["playwright"], fake_playwright)


class InventorySyncAutoFetchQboTest(unittest.TestCase):
    def setUp(self):
        self._inventory_apply_env = mock.patch.dict(
            os.environ,
            {"OIAT_ALLOW_INVENTORY_APPLY": "true"},
            clear=False,
        )
        self._inventory_apply_env.start()
        self.addCleanup(self._inventory_apply_env.stop)

    def _build_fake_config(self):
        cfg = mock.Mock()
        cfg.company_key = "company_a"
        cfg.display_name = "ACME"
        cfg.qbo_environment = "production"
        cfg.realm_id = "REALM123"
        cfg.inventory_max_qty_delta = None
        cfg.inventory_adjustment_account_id = ""
        cfg.slack_webhook_url = ""
        return cfg

    def test_stale_marker_with_same_timestamp_invalidates_snapshot(self):
        import json
        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            snapshot_dir = Path(td)
            snapshot_path = snapshot_dir / "company_a_products.csv"
            snapshot_path.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget 330ml,Inventory,true,5\n",
                encoding="utf-8",
            )
            snapshot_mtime = snapshot_path.stat().st_mtime
            invalidated_at = datetime.fromtimestamp(snapshot_mtime, tz=timezone.utc).isoformat()
            marker_path = snapshot_dir / "company_a_products.invalidate.json"
            marker_path.write_text(
                json.dumps({
                    "company_key": "company_a",
                    "reason": "unit_test",
                    "invalidated_at": invalidated_at,
                }),
                encoding="utf-8",
            )
            os.utime(marker_path, (snapshot_mtime, snapshot_mtime))

            with mock.patch.object(qbo_snapshot_cache, "qbo_snapshots_dir", return_value=snapshot_dir):
                reason = qbo_snapshot_cache.get_qbo_snapshot_stale_reason(
                    "company_a",
                    snapshot_path,
                )

        self.assertEqual(reason, "unit_test")

    def test_fetch_qbo_snapshot_reuses_fresh_cache_without_touching_tokens(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            output_path = Path(td) / "exports" / "company_a_products.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget 330ml,Inventory,true,5\n",
                encoding="utf-8",
            )

            with mock.patch.object(inventory_sync, "verify_realm_match") as mock_verify, \
                 mock.patch.object(inventory_sync, "TokenManager") as mock_token_mgr:
                result = inventory_sync.fetch_qbo_inventory_items_snapshot(
                    company_key="company_a",
                    realm_id="REALM123",
                    output_path=output_path,
                    cache_max_age_hours=24,
                    force_refresh=False,
                    enrich_starting_quantities=False,
                )

        self.assertEqual(result, output_path)
        mock_verify.assert_not_called()
        mock_token_mgr.assert_not_called()

    def test_fetch_qbo_snapshot_refreshes_when_cache_was_invalidated(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            snapshot_dir = temp_root / "exports" / "qbo_snapshots"
            output_path = snapshot_dir / "company_a_products.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Old Widget,Inventory,true,5\n",
                encoding="utf-8",
            )
            with mock.patch.object(qbo_snapshot_cache, "qbo_snapshots_dir", return_value=snapshot_dir), \
                 mock.patch.object(inventory_sync, "verify_realm_match") as mock_verify, \
                 mock.patch.object(inventory_sync, "TokenManager", return_value=mock.Mock()) as mock_token_mgr, \
                 mock.patch.object(
                     inventory_sync,
                     "_qbo_query_items_page",
                     side_effect=[
                         [{"Id": "11", "Name": "Fresh Widget", "Type": "Inventory", "TrackQtyOnHand": True, "QtyOnHand": 7}],
                         [],
                     ],
                 ) as mock_query:
                qbo_snapshot_cache.mark_qbo_snapshot_stale("company_a", reason="unit_test")
                result = inventory_sync.fetch_qbo_inventory_items_snapshot(
                    company_key="company_a",
                    realm_id="REALM123",
                    output_path=output_path,
                    cache_max_age_hours=24,
                    force_refresh=False,
                    enrich_starting_quantities=False,
                )

            self.assertEqual(result, output_path)
            self.assertEqual(mock_query.call_count, 1)
            self.assertTrue(mock_verify.called)
            self.assertTrue(mock_token_mgr.called)
            contents = output_path.read_text(encoding="utf-8")
            self.assertIn("Fresh Widget", contents)
            self.assertNotIn("Old Widget", contents)

    def test_fetch_qbo_snapshot_writes_diagnostic_columns_and_preserves_raw_name(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            output_path = Path(td) / "exports" / "company_a_products.csv"
            with mock.patch.object(inventory_sync, "verify_realm_match"), \
                 mock.patch.object(inventory_sync, "TokenManager", return_value=mock.Mock()), \
                 mock.patch.object(
                     inventory_sync,
                     "_qbo_query_items_page",
                     side_effect=[
                         [
                             {
                                 "Id": "9355",
                                 "Name": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml",
                                 "Type": "Inventory",
                                 "TrackQtyOnHand": True,
                                 "QtyOnHand": 10,
                                 "Active": True,
                                 "InvStartDate": "2026-01-01",
                                 "ParentRef": {"value": "12"},
                                 "SubItem": False,
                                 "UnitPrice": 1200,
                                 "PurchaseCost": 800,
                             }
                         ],
                         [],
                     ],
                 ):
                    inventory_sync.fetch_qbo_inventory_items_snapshot(
                        company_key="company_a",
                        realm_id="REALM123",
                        output_path=output_path,
                        force_refresh=True,
                        enrich_starting_quantities=False,
                    )

            rows = pd.read_csv(output_path)

        self.assertIn("Active", rows.columns)
        self.assertIn("InvStartDate", rows.columns)
        self.assertIn("ParentRef", rows.columns)
        self.assertIn("qbo_name_original", rows.columns)
        row = rows.iloc[0].to_dict()
        self.assertEqual(row["Name"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(row["qbo_name_original"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(row["qbo_name_display"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")

    def test_qbo_item_snapshot_query_does_not_select_subitem(self):
        from urllib.parse import parse_qs, unquote, urlparse

        class FakeResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"QueryResponse": {}}

        queries: list[str] = []

        def fake_request(_method, url, _token_mgr):
            raw_query = parse_qs(urlparse(url).query)["query"][0]
            queries.append(unquote(raw_query))
            return FakeResponse()

        with mock.patch.object(inventory_sync, "get_qbo_api_base_url", return_value="https://qbo.example"), \
             mock.patch.object(inventory_sync, "_make_qbo_request", side_effect=fake_request):
            inventory_sync._qbo_query_items_page(
                mock.Mock(),
                realm_id="REALM123",
                start_position=1,
                max_results=1000,
            )

        self.assertEqual(len(queries), 1)
        self.assertIn("ParentRef", queries[0])
        self.assertNotIn("SubItem", queries[0])

    def test_parse_inventory_valuation_detail_start_rows_by_item_id(self):
        payload = {
            "Rows": {
                "Row": [
                    {
                        "Header": {"ColData": [{"value": "BADIA COMPLETE SEASONING 340.2g", "id": "13706"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "2026-04-15"},
                                        {"value": "Inventory Starting Value", "id": "67623"},
                                        {"value": "START"},
                                        {"value": ""},
                                        {"value": "10.00"},
                                        {"value": "6500.00"},
                                        {"value": "65000.00"},
                                        {"value": "10.00"},
                                        {"value": "65000.00"},
                                    ],
                                    "type": "Data",
                                }
                            ]
                        },
                    },
                    {
                        "Header": {"ColData": [{"value": "BAMA-MAYONNAISE226ml*12", "id": "14641"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "2026-04-15"},
                                        {"value": "Inventory Starting Value"},
                                        {"value": "START"},
                                        {"value": ""},
                                        {"value": "2.00"},
                                        {"value": ""},
                                        {"value": ""},
                                        {"value": "2.00"},
                                        {"value": ""},
                                    ],
                                    "type": "Data",
                                }
                            ]
                        },
                    },
                    {
                        "Header": {"ColData": [{"value": "ZERO START", "id": "14700"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "2026-04-15"},
                                        {"value": "Inventory Starting Value"},
                                        {"value": "START"},
                                        {"value": ""},
                                        {"value": ".00"},
                                        {"value": "500.00"},
                                    ],
                                    "type": "Data",
                                }
                            ]
                        },
                    },
                    {
                        "Header": {"ColData": [{"value": "BEGINNING ONLY", "id": "14701"}]},
                        "Rows": {"Row": [{"ColData": [{"value": ""}, {"value": "Beginning Balance"}], "type": "Data"}]},
                    },
                ]
            }
        }

        parsed = inventory_sync.parse_inventory_valuation_starting_quantities(payload)

        self.assertEqual(parsed["13706"]["current_starting_qty"], 10.0)
        self.assertEqual(parsed["13706"]["rate"], 6500.0)
        self.assertEqual(parsed["13706"]["inventory_cost"], 65000.0)
        self.assertEqual(parsed["14641"]["current_starting_qty"], 2.0)
        self.assertEqual(parsed["14700"]["current_starting_qty"], 0.0)
        self.assertEqual(parsed["14701"]["status"], "beginning_balance_only")
        self.assertIsNone(parsed["14701"]["current_starting_qty"])

    def test_fetch_inventory_valuation_detail_uses_full_date_range(self):
        from urllib.parse import parse_qs, urlparse

        class FakeResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"Rows": {"Row": []}}

        seen_urls = []

        def fake_request(_method, url, _token_mgr):
            seen_urls.append(url)
            return FakeResponse()

        with mock.patch.object(inventory_sync, "get_qbo_api_base_url", return_value="https://qbo.example"), \
             mock.patch.object(inventory_sync, "_make_qbo_request", side_effect=fake_request):
            inventory_sync.fetch_qbo_inventory_starting_quantities(
                token_mgr=mock.Mock(),
                realm_id="REALM123",
            )

        params = parse_qs(urlparse(seen_urls[0]).query)
        self.assertEqual(params["start_date"], ["1900-01-01"])
        self.assertIn("end_date", params)
        self.assertEqual(params["minorversion"], [str(inventory_sync._QBO_MINOR_VERSION)])

    def test_fetch_qbo_snapshot_retries_safe_baseline_when_optional_field_rejected(self):
        import tempfile
        from pathlib import Path
        from urllib.parse import parse_qs, unquote, urlparse

        class FakeResponse:
            def __init__(self, status_code, text="", payload=None):
                self.status_code = status_code
                self.text = text
                self._payload = payload or {}

            def json(self):
                return self._payload

        queries: list[str] = []
        responses = [
            FakeResponse(
                400,
                "QueryValidationError: Property ParentRef not found for Entity Item",
            ),
            FakeResponse(
                200,
                payload={
                    "QueryResponse": {
                        "Item": [
                            {
                                "Id": "13875",
                                "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                                "Type": "Inventory",
                                "TrackQtyOnHand": True,
                                "QtyOnHand": -229,
                                "Active": True,
                            }
                        ]
                    }
                },
            ),
        ]

        def fake_request(_method, url, _token_mgr):
            raw_query = parse_qs(urlparse(url).query)["query"][0]
            queries.append(unquote(raw_query))
            return responses.pop(0)

        with tempfile.TemporaryDirectory() as td:
            output_path = Path(td) / "exports" / "company_a_products.csv"
            with mock.patch.object(inventory_sync, "verify_realm_match"), \
                 mock.patch.object(inventory_sync, "get_qbo_api_base_url", return_value="https://qbo.example"), \
                 mock.patch.object(inventory_sync, "_make_qbo_request", side_effect=fake_request), \
                 redirect_stdout(io.StringIO()) as buf:
                inventory_sync.fetch_qbo_inventory_items_snapshot(
                    company_key="company_a",
                    realm_id="REALM123",
                    token_mgr=mock.Mock(),
                    output_path=output_path,
                    force_refresh=True,
                    enrich_starting_quantities=False,
                )
            rows = pd.read_csv(output_path)

        self.assertEqual(len(queries), 2)
        self.assertIn("ParentRef", queries[0])
        self.assertNotIn("SubItem", queries[0])
        self.assertNotIn("ParentRef", queries[1])
        self.assertNotIn("InvStartDate", queries[1])
        self.assertIn("Active", queries[1])
        self.assertIn("retrying with safe baseline fields", buf.getvalue())
        row = rows.iloc[0].to_dict()
        self.assertEqual(row["Name"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")
        self.assertEqual(row["qbo_name_original"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")
        self.assertEqual(row["qbo_name_raw"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")

    def test_fetch_qbo_snapshot_enriches_starting_qty_from_inventory_valuation_detail(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            output_path = Path(td) / "exports" / "company_a_products.csv"
            with mock.patch.object(inventory_sync, "verify_realm_match"), \
                 mock.patch.object(inventory_sync, "TokenManager", return_value=mock.Mock()), \
                 mock.patch.object(
                     inventory_sync,
                     "_qbo_query_items_page",
                     side_effect=[
                         [
                             {
                                 "Id": "13706",
                                 "Name": "BADIA COMPLETE SEASONING 340.2g",
                                 "Type": "Inventory",
                                 "TrackQtyOnHand": True,
                                 "QtyOnHand": -217,
                                 "Active": True,
                             }
                         ],
                         [],
                     ],
                 ), \
                 mock.patch.object(
                     inventory_sync,
                     "fetch_qbo_inventory_starting_quantities",
                     return_value={
                         "13706": {
                             "current_starting_qty": 10.0,
                             "rate": 6500.0,
                             "inventory_cost": 65000.0,
                             "asset_value": 65000.0,
                             "source": "inventory_valuation_detail_start_row",
                             "status": "found",
                         }
                     },
                 ):
                    inventory_sync.fetch_qbo_inventory_items_snapshot(
                        company_key="company_a",
                        realm_id="REALM123",
                        output_path=output_path,
                        force_refresh=True,
                    )

            rows = pd.read_csv(output_path)

        row = rows.iloc[0].to_dict()
        self.assertEqual(row["qbo_current_starting_qty"], 10)
        self.assertEqual(row["qbo_starting_qty_rate"], 6500)
        self.assertEqual(row["qbo_starting_qty_source"], "inventory_valuation_detail_start_row")
        self.assertEqual(row["qbo_starting_qty_status"], "found")

    def test_auto_fetch_qbo_writes_default_path_and_uses_it(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        recorded: dict = {}

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "company_a_StockReport.csv"
            stock_csv.write_text(
                "Name,MeasuredCurrentStock\nWidget 330ml,5\n",
                encoding="utf-8",
            )
            report_path = tdp / "report.csv"
            qbo_written = tdp / "exports" / "qbo_snapshots" / "company_a_products.csv"
            qbo_written.parent.mkdir(parents=True, exist_ok=True)

            def fake_fetch(**kwargs):
                recorded.update(kwargs)
                qbo_written.write_text(
                    "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                    "10,Widget 330ml,Inventory,true,5\n",
                    encoding="utf-8",
                )
                return qbo_written

            buf = io.StringIO()
            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "_default_qbo_export_write_path", return_value=qbo_written), \
                 mock.patch.object(inventory_sync, "fetch_qbo_inventory_items_snapshot", side_effect=fake_fetch), \
                 redirect_stdout(buf):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--auto-fetch-qbo",
                    "--output", str(report_path),
                ])

        self.assertEqual(exit_code, 0)
        self.assertEqual(recorded.get("company_key"), "company_a")
        self.assertEqual(recorded.get("realm_id"), "REALM123")
        self.assertTrue(str(qbo_written).endswith("company_a_products.csv"))
        self.assertIn("Inventory audit", buf.getvalue())

    def test_explicit_qbo_csv_skips_auto_fetch(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        invoked = {"value": False}

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "company_a_StockReport.csv"
            stock_csv.write_text("Name,MeasuredCurrentStock\nWidget 330ml,5\n", encoding="utf-8")
            qbo_csv = tdp / "manual_qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget 330ml,Inventory,true,5\n",
                encoding="utf-8",
            )
            report_path = tdp / "report.csv"

            def fake_fetch(**_kwargs):
                invoked["value"] = True
                return qbo_csv

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "fetch_qbo_inventory_items_snapshot", side_effect=fake_fetch), \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--auto-fetch-qbo",
                    "--qbo-csv", str(qbo_csv),
                    "--output", str(report_path),
                ])

        self.assertEqual(exit_code, 0)
        self.assertFalse(invoked["value"])

    def test_force_refresh_passes_flag_through(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        recorded: dict = {}

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "company_a_StockReport.csv"
            stock_csv.write_text("Name,MeasuredCurrentStock\nWidget 330ml,5\n", encoding="utf-8")
            report_path = tdp / "report.csv"
            qbo_written = tdp / "exports" / "qbo_snapshots" / "company_a_products.csv"
            qbo_written.parent.mkdir(parents=True, exist_ok=True)

            def fake_fetch(**kwargs):
                recorded.update(kwargs)
                qbo_written.write_text(
                    "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                    "10,Widget 330ml,Inventory,true,5\n",
                    encoding="utf-8",
                )
                return qbo_written

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 mock.patch.object(inventory_sync, "_default_qbo_export_write_path", return_value=qbo_written), \
                 mock.patch.object(inventory_sync, "fetch_qbo_inventory_items_snapshot", side_effect=fake_fetch), \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--auto-fetch-qbo",
                    "--qbo-force-refresh",
                    "--qbo-cache-max-age-hours", "24",
                    "--output", str(report_path),
                ])

        self.assertEqual(exit_code, 0)
        self.assertTrue(recorded.get("force_refresh"))
        self.assertEqual(recorded.get("cache_max_age_hours"), 24)

    def test_apply_is_removed_and_does_not_mark_qbo_snapshot_stale(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        fake_config.inventory_adjustment_account_id = "88"

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text("Name,CategoryName,MeasuredCurrentStock\nWidget,ALCOHOLS,5\n", encoding="utf-8")
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,Widget,Inventory,true,1\n",
                encoding="utf-8",
            )

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 redirect_stdout(buf):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--qbo-csv", str(qbo_csv),
                    "--apply",
                    "--max-adjustments", "1",
                ])

        self.assertEqual(exit_code, 2)
        self.assertIn("inventory quantity apply has been removed", buf.getvalue())

    def test_dry_run_prints_manual_starting_value_preview(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        fake_config.inventory_adjustment_account_id = "88"

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "33 EXPORT LAGER BEER CAN 50cl,ALCOHOLS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "9124,33 EXPORT LAGER BEER CAN 50cl,Inventory,true,1\n",
                encoding="utf-8",
            )

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 redirect_stdout(buf):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--qbo-csv", str(qbo_csv),
                    "--dry-run",
                    "--adjust-account-id", "88",
                    "--txn-date", "2026-04-28",
                    "--max-adjustments", "1",
                ])
            self.assertEqual(exit_code, 0)
            output = buf.getvalue()
            self.assertIn("Manual starting-value correction preview", output)
            self.assertIn("item_id=9124", output)
            self.assertIn("delta=7.0", output)

    def test_dry_run_previews_fallback_rows(self):
        import tempfile
        from pathlib import Path

        fake_config = self._build_fake_config()
        fake_config.inventory_adjustment_account_id = "88"

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GADGET,ALCOHOLS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,GADGET*12,Inventory,true,1\n",
                encoding="utf-8",
            )

            env = {**os.environ, "OIAT_RUNTIME_ENV": "production"}
            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(inventory_sync, "load_company_config", return_value=fake_config), \
                 mock.patch.object(inventory_sync, "get_available_companies", return_value=["company_a"]), \
                 redirect_stdout(buf):
                exit_code = inventory_sync.main([
                    "--company", "company_a",
                    "--stock-csv", str(stock_csv),
                    "--qbo-csv", str(qbo_csv),
                    "--dry-run",
                    "--adjust-account-id", "88",
                    "--txn-date", "2026-04-28",
                    "--max-adjustments", "10",
                ])
            self.assertEqual(exit_code, 0)
            # The payload's PrivateNote includes the pick method.
            self.assertIn("pick=fallback_largest_qty", buf.getvalue())

    def test_catalog_issue_classification_only_pack_variant_exists(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "BACARDI WHITE RUM 750ml,ALCOHOLS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,BACARDI WHITE RUM 750ml*12,Inventory,true,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertEqual(row["catalog_issue_type"], "only_pack_variant_exists")
        self.assertIn("only pack variant exists in QuickBooks", row["catalog_issue_detail"])
        self.assertIn("*12", row["catalog_issue_detail"])
        self.assertIn("create base item", row["suggested_next_action"])
        self.assertIn("qbo_item_names_for_base", report.columns)
        self.assertIn("qbo_pack_variant_names_for_base", report.columns)
        self.assertIn("BACARDI WHITE RUM 750ml*12", str(row.get("qbo_pack_variant_names_for_base")))
        self.assertEqual(row["qbo_current_starting_qty"], "")
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "8")
        self.assertEqual(row["qbo_starting_qty_status"], "create_base_item_initial_qty")

    def test_catalog_issue_classification_base_with_pack_variants(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GOLDBERG CAN 50cl,DRINKS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,GOLDBERG CAN 50cl,Inventory,true,1\n"
                "11,GOLDBERG CAN 50cl*6,Inventory,true,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertEqual(row["catalog_issue_type"], "base_with_pack_variants")
        self.assertIn("consolidate pack variants", row["catalog_issue_detail"])
        self.assertIn("GOLDBERG CAN 50cl*6", row["catalog_issue_detail"])
        self.assertIn("consolidation and cleanup", row["suggested_next_action"])

    def test_product_filter_treats_pack_multiplier_as_literal_text(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,2\n"
                "OTHER ITEM,ALCOHOLS,7\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="GOLDBERG CAN 50cl*24",
            )

        self.assertEqual(len(epos), 1)
        row = epos.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "GOLDBERG CAN 50cl")
        self.assertEqual(row["epos_single_units"], 48)

    def test_pack_multiplier_normalization_includes_loose_units_from_current_volume(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,25,23 of 24 Each,25.958\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))

        self.assertEqual(len(epos), 1)
        row = epos.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "GOLDBERG CAN 50cl")
        self.assertEqual(row["epos_single_units"], 623.0)

    def test_pack_multiplier_normalization_current_volume_zero(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,25,0 of 24 Each,25.958\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))

        self.assertEqual(len(epos), 1)
        self.assertEqual(epos.iloc[0]["epos_single_units"], 600.0)

    def test_pack_multiplier_normalization_falls_back_to_total_stock_when_current_volume_missing(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Total Stock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,25,25.958\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))

        self.assertEqual(len(epos), 1)
        self.assertEqual(epos.iloc[0]["epos_single_units"], 623.0)

    def test_negative_epos_pack_row_is_clamped_before_grouping(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "ACTION BITTERS50ml*20,ALCOHOLS & SPIRITS,0,15 of 20 Each,0.75\n"
                "ACTION BITTERS50ml*120,ALCOHOLS & SPIRITS,0,-30 of 120 Each,-0.25\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,ACTION BITTERS50ml,Inventory,true,15\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo)

        self.assertEqual(len(epos), 1)
        row = epos.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "ACTION BITTERS50ml")
        self.assertEqual(row["epos_single_units"], 15.0)
        self.assertEqual(row["epos_negative_rows_clamped"], 1)
        self.assertEqual(row["epos_negative_units_clamped"], 30.0)
        self.assertEqual(row["epos_negative_stock_policy"], inventory_sync.EPOS_NEGATIVE_STOCK_POLICY)
        self.assertIn("ACTION BITTERS50ml*120", row["epos_negative_clamped_row_names"])
        audit_row = report.iloc[0].to_dict()
        self.assertEqual(audit_row["epos_single_units"], 15.0)
        self.assertEqual(audit_row["epos_negative_rows_clamped"], 1)
        self.assertEqual(audit_row["epos_negative_units_clamped"], 30.0)

    def test_negative_epos_non_pack_stock_is_clamped_before_grouping(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "SIMPLE BITTERS 50ml,ALCOHOLS & SPIRITS,-5\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))

        self.assertEqual(len(epos), 1)
        row = epos.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "SIMPLE BITTERS 50ml")
        self.assertEqual(row["epos_single_units"], 0.0)
        self.assertEqual(row["epos_negative_rows_clamped"], 1)
        self.assertEqual(row["epos_negative_units_clamped"], 5.0)
        self.assertEqual(row["epos_negative_stock_policy"], inventory_sync.EPOS_NEGATIVE_STOCK_POLICY)

    def test_product_filter_with_pack_multiplier_still_literal_text_with_volume_columns(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,25,23 of 24 Each,25.958\n"
                "OTHER ITEM,ALCOHOLS,7,,7\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="GOLDBERG CAN 50cl*24",
            )

        self.assertEqual(len(epos), 1)
        row = epos.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "GOLDBERG CAN 50cl")
        self.assertEqual(row["epos_single_units"], 623.0)

    def test_case_insensitive_base_matching_classifies_existing_base_with_pack_variants(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "LEGEND EXTRA STOUT CAN 440ml*24,ALCOHOLS,1\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand,qbo_current_starting_qty,qbo_starting_qty_source,qbo_starting_qty_status\n"
                "10,LEGEND EXTRA STOUT CAN 440ML,Inventory,true,-299,10,inventory_valuation_detail_start_row,found\n"
                "11,LEGEND EXTRA STOUT CAN 440ml*12,Inventory,true,10,5,inventory_valuation_detail_start_row,found\n"
                "12,LEGEND EXTRA STOUT CAN 440ml*24,Inventory,true,20,6,inventory_valuation_detail_start_row,found\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="LEGEND EXTRA STOUT CAN 440ml*24",
            )
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        self.assertEqual(len(report), 1)
        row = report.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "LEGEND EXTRA STOUT CAN 440ml")
        self.assertEqual(row["catalog_issue_type"], "base_with_pack_variants")
        self.assertIn("LEGEND EXTRA STOUT CAN 440ML", row["qbo_base_item_names_for_base"])
        self.assertIn("LEGEND EXTRA STOUT CAN 440ml*24", row["qbo_pack_variant_names_for_base"])
        self.assertNotEqual(row["catalog_issue_type"], "only_pack_variant_exists")
        self.assertEqual(row["qbo_current_starting_qty"], "10")
        self.assertEqual(row["qbo_base_qty_on_hand"], -299.0)
        self.assertEqual(row["qbo_pack_variant_qty_on_hand"], 30.0)
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "333")
        self.assertIn("LEGEND EXTRA STOUT CAN 440ml*12: set New Initial Qty -5", row["qbo_pack_variant_starting_value_plan"])
        self.assertIn("LEGEND EXTRA STOUT CAN 440ml*24: set New Initial Qty -14", row["qbo_pack_variant_starting_value_plan"])

    def test_ml_casing_variants_normalize_to_single_qbo_base_group(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,LEGEND EXTRA STOUT CAN 440ML,Inventory,true,1\n"
                "11,LEGEND EXTRA STOUT CAN 440ml,Inventory,true,2\n"
                "12,LEGEND EXTRA STOUT CAN 440Ml*24,Inventory,true,3\n",
                encoding="utf-8",
            )
            grouped = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))

        self.assertEqual(len(grouped), 1)
        row = grouped.iloc[0].to_dict()
        self.assertEqual(row["qbo_item_count_for_base"], 3)
        self.assertTrue(row["qbo_has_pack_variants"])

    def test_qbo_duplicate_rows_by_id_are_deduped_before_classification(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,ALCOHOLS,1\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        self.assertEqual(len(report), 1)
        row = report.iloc[0].to_dict()
        self.assertEqual(row["catalog_issue_type"], "base_with_pack_variants")
        self.assertNotEqual(row["catalog_issue_type"], "multiple_active_base_items")
        self.assertEqual(row["qbo_item_count_for_base"], 3)
        self.assertEqual(row["qbo_base_item_count"], 1)
        self.assertEqual(
            row["qbo_item_names_for_base"],
            "SMIRNOFF ICE DOUBLE BLACK CAN 330ml | SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12 | SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24",
        )
        self.assertEqual(row["qbo_base_item_ids"], "13875")

    def test_qbo_item_row_loader_dedupes_duplicate_ids(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,ITEM A,Inventory,true,1\n"
                "10,ITEM A,Inventory,true,1\n"
                "11,ITEM A*12,Inventory,true,2\n",
                encoding="utf-8",
            )
            rows = inventory_sync.load_qbo_inventory_item_rows(str(qbo_csv))

        self.assertEqual(len(rows), 2)
        self.assertEqual(set(rows["Id"].tolist()), {"10", "11"})

    def test_qbo_item_row_loader_preserves_original_name_spacing(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "9355,SMIRNOFF ICE DOUBLE BLACK  CAN 330ml,Inventory,true,10\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n",
                encoding="utf-8",
            )
            rows = inventory_sync.load_qbo_inventory_item_rows(str(qbo_csv))

        typo = rows[rows["Id"] == "9355"].iloc[0]
        canonical = rows[rows["Id"] == "13875"].iloc[0]
        self.assertEqual(typo["Name"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_original"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_raw"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_display"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")
        self.assertEqual(canonical["Name"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")
        self.assertEqual(typo["base_name"], canonical["base_name"])

    def test_qbo_grouped_snapshot_keeps_original_name_field_separate_from_display(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "9355,SMIRNOFF ICE DOUBLE BLACK  CAN 330ml,Inventory,true,10\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n",
                encoding="utf-8",
            )
            rows = inventory_sync.load_qbo_inventory_item_rows(str(qbo_csv))
            grouped = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))

        self.assertIn("qbo_name_original", rows.columns)
        self.assertEqual(rows[rows["Id"] == "9355"].iloc[0]["qbo_name_original"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped.iloc[0]["qbo_base_item_names_for_base"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")
        self.assertEqual(grouped.iloc[0]["qbo_base_item_ids"], "9355,13875")

    def test_qbo_report_columns_show_rows_unique_names_and_active_base_ids(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "SMIRNOFF ICE DOUBLE BLACK CAN 330ml,ALCOHOLS,0\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand,Active\n"
                "9355,SMIRNOFF ICE DOUBLE BLACK  CAN 330ml,Inventory,true,10,true\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229,true\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1,true\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2,true\n"
                "1,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,0,false\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertIn("qbo_item_row_count_for_base", report.columns)
        self.assertIn("qbo_unique_item_count_for_base", report.columns)
        self.assertIn("qbo_active_base_item_count", report.columns)
        self.assertEqual(row["qbo_item_row_count_for_base"], 4)
        self.assertEqual(row["qbo_unique_item_count_for_base"], 3)
        self.assertEqual(row["qbo_active_base_item_count"], 2)
        self.assertEqual(row["qbo_active_pack_variant_count"], 2)
        self.assertEqual(row["qbo_base_item_ids"], "9355,13875")
        self.assertEqual(row["qbo_active_base_item_ids"], "9355,13875")
        self.assertEqual(row["qbo_inactive_base_item_ids"], "1")
        self.assertEqual(row["qbo_active_pack_variant_item_ids"], "13956,13942")
        self.assertNotIn("13956", row["qbo_base_item_ids"])

    def test_qbo_grouped_quantity_uses_raw_qtyonhand_without_pack_multiplier_scaling(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2\n",
                encoding="utf-8",
            )
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))

        self.assertEqual(len(qbo), 1)
        # Current audit intent is to aggregate raw per-item QtyOnHand values.
        self.assertEqual(float(qbo.iloc[0]["qbo_qty_on_hand"]), -232.0)

    def test_current_qbo_snapshot_output_ignores_stale_prior_ids(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,ALCOHOLS,1\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo_current.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2\n",
                encoding="utf-8",
            )
            # Simulate prior/stale IDs that must not leak into current grouping.
            stale_prior = pd.DataFrame(
                [{"qbo_base_item_ids": "9355,13702,13703,13875,13956,13942"}]
            )
            self.assertIn("9355", stale_prior.iloc[0]["qbo_base_item_ids"])

            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertEqual(row["qbo_item_count_for_base"], 3)
        self.assertEqual(row["qbo_base_item_count"], 1)
        self.assertEqual(row["qbo_base_item_ids"], "13875")
        self.assertNotIn("9355", row["qbo_base_item_ids"])
        self.assertEqual(row["catalog_issue_type"], "base_with_pack_variants")
        self.assertNotEqual(row["catalog_issue_type"], "multiple_active_base_items")

    def test_product_filter_with_pack_multiplier_is_case_insensitive_and_literal(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "LEGEND EXTRA STOUT CAN 440ml*24,ALCOHOLS,1\n"
                "OTHER STOUT,ALCOHOLS,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="legend extra stout can 440ML*24",
            )

        self.assertEqual(len(epos), 1)
        self.assertEqual(epos.iloc[0]["base_name"], "LEGEND EXTRA STOUT CAN 440ml")
        self.assertEqual(epos.iloc[0]["epos_single_units"], 24.0)

    def test_post_catalog_audit_needs_exact_quantity_adjustment_for_loose_pack_remainder(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,25,23 of 24 Each,25.958\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,GOLDBERG CAN 50cl,Inventory,true,600\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        self.assertEqual(len(report), 1)
        row = report.iloc[0].to_dict()
        self.assertEqual(row["catalog_issue_type"], "exact_name_match")
        self.assertEqual(row["epos_single_units"], 623.0)
        self.assertEqual(row["qbo_qty_on_hand"], 600.0)
        self.assertEqual(row["delta"], 23.0)
        self.assertEqual(row["qbo_current_starting_qty"], "")
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "")
        self.assertEqual(row["qbo_starting_qty_source"], "")
        self.assertEqual(row["status"], "needs_adjustment")

    def test_audit_report_calculates_new_initial_qty_from_qbo_starting_qty(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "REAL PRODUCT,PROVISIONS,1039\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand,qbo_current_starting_qty,qbo_starting_qty_source,qbo_starting_qty_status\n"
                "13706,REAL PRODUCT,Inventory,true,-217,10,inventory_valuation_detail_start_row,found\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertEqual(row["delta"], 1256.0)
        self.assertEqual(row["qbo_current_starting_qty"], "10")
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "1266")
        self.assertEqual(row["qbo_starting_qty_status"], "found")

    def test_product_filter_with_regex_chars_is_literal_text(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "SPECIAL (CAN)+[TEST]*24,ALCOHOLS,1\n"
                "SPECIAL CAN TEST,ALCOHOLS,9\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="SPECIAL (CAN)+[TEST]*24",
            )

        self.assertEqual(len(epos), 1)
        self.assertEqual(epos.iloc[0]["base_name"], "SPECIAL (CAN)+[TEST]")
        self.assertEqual(epos.iloc[0]["epos_single_units"], 24)

    def test_catalog_issue_classification_with_one_pack_multiplier_row(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,2\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,GOLDBERG CAN 50cl*24,Inventory,true,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="GOLDBERG CAN 50cl*24",
            )
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        self.assertEqual(len(report), 1)
        row = report.iloc[0].to_dict()
        self.assertEqual(row["base_name"], "GOLDBERG CAN 50cl")
        self.assertEqual(row["catalog_issue_type"], "only_pack_variant_exists")
        self.assertIsInstance(row["catalog_issue_type"], str)
        self.assertIn("GOLDBERG CAN 50cl*24", row["catalog_issue_detail"])
        self.assertEqual(row["qbo_current_starting_qty"], "")
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "48")
        self.assertEqual(row["qbo_starting_qty_status"], "create_base_item_initial_qty")

    def test_empty_product_filter_result_keeps_scalar_catalog_columns(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS,2\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,GOLDBERG CAN 50cl*24,Inventory,true,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(
                str(stock_csv),
                product_filter="NOT PRESENT * [x]",
            )
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        self.assertTrue(report.empty)
        self.assertIn("catalog_issue_type", report.columns)
        self.assertIn("catalog_issue_detail", report.columns)

    def test_catalog_issue_classification_missing_from_qbo(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            stock_csv = tdp / "stock.csv"
            stock_csv.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "NEW EPOS ITEM,DRINKS,8\n",
                encoding="utf-8",
            )
            qbo_csv = tdp / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,SOME OTHER ITEM,Inventory,true,1\n",
                encoding="utf-8",
            )
            epos = inventory_sync.load_epos_stock_snapshot(str(stock_csv))
            qbo = inventory_sync.load_qbo_inventory_snapshot(str(qbo_csv))
            report = inventory_sync.build_audit_report(epos, qbo, tolerance=0.0)

        row = report.iloc[0].to_dict()
        self.assertEqual(row["catalog_issue_type"], "missing_from_qbo")
        self.assertIn("product not found in QuickBooks", row["catalog_issue_detail"])
        self.assertIn("create inventory item", row["suggested_next_action"])
        self.assertEqual(row["qbo_current_starting_qty"], "")
        self.assertEqual(row["qbo_new_initial_qty_to_enter"], "8")
        self.assertEqual(row["qbo_starting_qty_status"], "create_item_initial_qty")


if __name__ == "__main__":
    unittest.main()
