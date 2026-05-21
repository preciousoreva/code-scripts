from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest import mock

from code_scripts.inventory_safety import (
    ALLOW_INVENTORY_APPLY_ENV,
    INVENTORY_APPLY_DISABLED_MESSAGE,
    InventoryApplyDisabledError,
    assert_inventory_apply_allowed,
)


class InventorySafetyGuardTests(unittest.TestCase):
    def test_allows_non_production_environments(self):
        cfg = SimpleNamespace(qbo_environment="sandbox")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "code_scripts.inventory_safety.get_runtime_qbo_environment",
            return_value="sandbox",
        ):
            assert_inventory_apply_allowed(cfg, action="test")

    def test_blocks_production_without_override(self):
        cfg = SimpleNamespace(qbo_environment="production")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "code_scripts.inventory_safety.get_runtime_qbo_environment",
            return_value="sandbox",
        ):
            with self.assertRaises(InventoryApplyDisabledError) as ctx:
                assert_inventory_apply_allowed(cfg, action="test")

        self.assertEqual(str(ctx.exception), INVENTORY_APPLY_DISABLED_MESSAGE)

    def test_blocks_when_runtime_environment_is_production(self):
        cfg = SimpleNamespace(qbo_environment="sandbox")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "code_scripts.inventory_safety.get_runtime_qbo_environment",
            return_value="production",
        ):
            with self.assertRaises(InventoryApplyDisabledError):
                assert_inventory_apply_allowed(cfg, action="test")

    def test_allows_production_with_override(self):
        cfg = SimpleNamespace(qbo_environment="production")
        with mock.patch.dict(os.environ, {ALLOW_INVENTORY_APPLY_ENV: "1"}, clear=True), mock.patch(
            "code_scripts.inventory_safety.get_runtime_qbo_environment",
            return_value="production",
        ):
            assert_inventory_apply_allowed(cfg, action="test")
