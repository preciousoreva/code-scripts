from __future__ import annotations

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse


class CoreViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")

    @override_settings(
        PORTAL_SOLUTIONS=[
            {
                "name": "EPOS -> QBO",
                "description": "Main dashboard",
                "url_name": "epos_qbo:overview",
            },
            {"name": "Invalid Missing URL", "description": "skip me"},
        ]
    )
    def test_home_renders_solutions_from_settings(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("core-home"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("EPOS -&gt; QBO", html)
        self.assertIn(reverse("epos_qbo:overview"), html)
        self.assertNotIn("Invalid Missing URL", html)

    def test_coming_soon_known_feature(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("core-coming-soon", kwargs={"feature": "mappings"}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mappings")
        self.assertContains(response, "Coming Soon")

    def test_account_requires_login(self):
        response = self.client.get(reverse("core-account"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_account_renders_for_authenticated_user(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("core-account"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "operator")
        self.assertContains(response, "Change password")

    def test_core_pages_do_not_leak_template_tokens(self):
        self.client.login(username="operator", password="pw12345")
        urls = [
            reverse("core-home"),
            reverse("core-coming-soon", kwargs={"feature": "settings"}),
            reverse("core-account"),
        ]
        for url in urls:
            response = self.client.get(url)
            self.assertEqual(response.status_code, 200)
            html = response.content.decode("utf-8")
            self.assertNotIn("{{", html)
            self.assertNotIn("{%", html)
            self.assertNotIn("{#", html)

    def test_coming_soon_unknown_feature_returns_404(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("core-coming-soon", kwargs={"feature": "not-real"}))
        self.assertEqual(response.status_code, 404)

    def test_coming_soon_requires_login(self):
        response = self.client.get(reverse("core-coming-soon", kwargs={"feature": "settings"}))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)
