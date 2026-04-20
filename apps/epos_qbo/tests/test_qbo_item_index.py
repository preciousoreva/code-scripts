from __future__ import annotations

from django.test import SimpleTestCase

from code_scripts.qbo_upload import _CaseInsensitiveItemIndex


class CaseInsensitiveItemIndexTests(SimpleTestCase):
    def test_lookup_matches_different_casing(self):
        idx = _CaseInsensitiveItemIndex()
        idx["CEDAA YOGHURT DRINK 50CL"] = {"Id": "42", "Name": "CEDAA YOGHURT DRINK 50CL"}

        self.assertIn("cedaa yoghurt drink 50cl", idx)
        self.assertEqual(idx.get("CEDAA YOGHURT DRINK 50cl")["Id"], "42")
        self.assertEqual(idx["Cedaa Yoghurt Drink 50Cl"]["Id"], "42")

    def test_missing_key_returns_default(self):
        idx = _CaseInsensitiveItemIndex()
        idx["FOO"] = {"Id": "1"}
        self.assertIsNone(idx.get("BAR"))
        self.assertNotIn("bar", idx)

    def test_stored_key_preserves_original_casing(self):
        idx = _CaseInsensitiveItemIndex()
        idx["MixedCase Name"] = {"Id": "7"}
        self.assertEqual(list(idx.keys()), ["MixedCase Name"])
