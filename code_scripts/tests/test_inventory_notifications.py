import unittest

from code_scripts.inventory_notifications import (
    format_inventory_audit_summary,
    format_pack_variant_apply_summary,
    format_scope,
)


class FormatScopeTest(unittest.TestCase):
    def test_no_filters_returns_empty_string(self):
        self.assertEqual(format_scope(), "")
        self.assertEqual(format_scope(category=[], product=""), "")

    def test_category_string(self):
        self.assertEqual(
            format_scope(category="ALCOHOLS & SPIRITS"),
            "category=ALCOHOLS & SPIRITS",
        )

    def test_category_list_joins_with_comma(self):
        self.assertEqual(
            format_scope(category=["ALCOHOLS & SPIRITS", "DRINKS & BEVERAGES"]),
            "category=ALCOHOLS & SPIRITS, DRINKS & BEVERAGES",
        )

    def test_product_only(self):
        self.assertEqual(format_scope(product="TROPHY"), "product=TROPHY")

    def test_both_separated_by_semicolon(self):
        self.assertEqual(
            format_scope(category=["X"], product="Y"),
            "category=X; product=Y",
        )


class InventoryAuditSummaryTest(unittest.TestCase):
    def test_audit_summary_includes_required_fields(self):
        msg = format_inventory_audit_summary(
            company_display_name="AKPONORA VENTURES LTD.",
            company_key="company_a",
            mode="audit",
            scope="category=ALCOHOLS & SPIRITS",
            counts={
                "total_groups": 134,
                "in_sync": 41,
                "needs_adjustment": 12,
                "ambiguous_in_qbo": 60,
                "missing_in_qbo": 8,
            },
            report_path="/data/.../inventory_audit_company_a_120000.csv",
            warnings_count=68,
        )
        self.assertIn("Inventory audit completed", msg)
        self.assertIn("AKPONORA VENTURES LTD. (company_a)", msg)
        self.assertIn("Mode: audit", msg)
        self.assertIn("Scope: category=ALCOHOLS & SPIRITS", msg)
        self.assertIn("total_groups=134", msg)
        self.assertIn("in_sync=41", msg)
        self.assertIn("needs_adjustment=12", msg)
        self.assertIn("Warnings / manual review: 68", msg)
        self.assertIn("inventory_audit_company_a_120000.csv", msg)

    def test_dry_run_label_renders_as_preview(self):
        msg = format_inventory_audit_summary(
            company_display_name="Co A", company_key="company_a",
            mode="dry-run",
            counts={"posted": 5, "skipped": 0},
        )
        self.assertIn("Inventory audit preview", msg)
        self.assertIn("Mode: dry-run", msg)

    def test_failure_branch_includes_error_and_red_x(self):
        msg = format_inventory_audit_summary(
            company_display_name="Co A", company_key="company_a",
            mode="apply",
            counts={"posted": 2, "skipped": 1},
            error="HTTP 400: validation",
            report_path="/r.csv",
        )
        self.assertTrue(msg.startswith("❌"))
        self.assertIn("Inventory audit failed", msg)
        self.assertIn("Error: HTTP 400: validation", msg)

    def test_zero_counts_are_kept_but_none_is_skipped(self):
        msg = format_inventory_audit_summary(
            company_display_name="Co A", company_key="company_a",
            mode="audit",
            counts={"in_sync": 0, "needs_adjustment": None, "missing_in_qbo": ""},
        )
        self.assertIn("in_sync=0", msg)
        self.assertNotIn("needs_adjustment=", msg)
        self.assertNotIn("missing_in_qbo=", msg)


class PackVariantApplySummaryTest(unittest.TestCase):
    def test_consolidation_summary_includes_required_fields(self):
        msg = format_pack_variant_apply_summary(
            kind="pack_variant_consolidation",
            company_display_name="AKPONORA VENTURES LTD.",
            company_key="company_a",
            mode="apply",
            scope="product=TROPHY",
            counts={
                "attempted": 3,
                "succeeded": 3,
                "failed": 0,
                "no_op": 0,
                "blocked": 1,
                "skipped_due_to_cap": 5,
            },
            report_path="/data/.../report.csv",
        )
        self.assertIn("Pack-variant consolidation completed", msg)
        self.assertIn("AKPONORA VENTURES LTD. (company_a)", msg)
        self.assertIn("Mode: apply", msg)
        self.assertIn("Scope: product=TROPHY", msg)
        self.assertIn("attempted=3", msg)
        self.assertIn("succeeded=3", msg)
        self.assertIn("blocked=1", msg)
        self.assertIn("skipped_due_to_cap=5", msg)
        self.assertTrue(msg.startswith("✅"))

    def test_cleanup_summary_uses_cleanup_title(self):
        msg = format_pack_variant_apply_summary(
            kind="pack_variant_cleanup",
            company_display_name="Co A", company_key="company_a",
            mode="apply",
            counts={"attempted": 5, "succeeded": 5, "failed": 0,
                    "skipped_due_to_cap": 0},
            report_path="/r.csv",
        )
        self.assertIn("Pack-variant cleanup completed", msg)

    def test_invalid_kind_rejected(self):
        with self.assertRaises(ValueError):
            format_pack_variant_apply_summary(
                kind="inventory_audit",  # wrong helper for this kind
                company_display_name="X", company_key="x",
                mode="apply",
            )

    def test_failure_branch_includes_error_and_red_x(self):
        msg = format_pack_variant_apply_summary(
            kind="pack_variant_consolidation",
            company_display_name="Co A", company_key="company_a",
            mode="apply",
            counts={"attempted": 1, "succeeded": 0, "failed": 1},
            error="HTTP 401",
        )
        self.assertTrue(msg.startswith("❌"))
        self.assertIn("Pack-variant consolidation failed", msg)
        self.assertIn("Error: HTTP 401", msg)

    def test_some_failed_uses_warning_emoji_when_no_top_level_error(self):
        msg = format_pack_variant_apply_summary(
            kind="pack_variant_consolidation",
            company_display_name="Co A", company_key="company_a",
            mode="apply",
            counts={"attempted": 3, "succeeded": 2, "failed": 1},
        )
        # No top-level `error=...`, but failed > 0 -> warning emoji.
        self.assertTrue(msg.startswith("⚠️"))


if __name__ == "__main__":
    unittest.main()
