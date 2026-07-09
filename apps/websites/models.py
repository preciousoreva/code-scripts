from __future__ import annotations

import secrets

from django.conf import settings
from django.db import models
from django.urls import reverse
from django.utils import timezone


def generate_ingest_secret() -> str:
    return secrets.token_urlsafe(32)


class Website(models.Model):
    PLATFORM_WIX = "wix"
    PLATFORM_WORDPRESS = "wordpress"
    PLATFORM_STATIC = "static"
    PLATFORM_OTHER = "other"
    PLATFORM_CHOICES = [
        (PLATFORM_WIX, "Wix Studio"),
        (PLATFORM_WORDPRESS, "WordPress"),
        (PLATFORM_STATIC, "Static"),
        (PLATFORM_OTHER, "Other"),
    ]

    slug = models.SlugField(max_length=80, unique=True)
    name = models.CharField(max_length=255)
    domain = models.CharField(max_length=255, unique=True)
    platform = models.CharField(max_length=32, choices=PLATFORM_CHOICES, default=PLATFORM_WIX)
    public_url = models.URLField(blank=True)
    log_ingest_secret = models.CharField(max_length=96, default=generate_ingest_secret, unique=True)
    is_active = models.BooleanField(default=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        permissions = [
            ("can_manage_websites", "Can manage website monitoring"),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.domain})"

    @property
    def display_url(self) -> str:
        if self.public_url:
            return self.public_url
        return f"https://{self.domain}"

    def wix_ingest_path(self) -> str:
        return reverse(
            "websites:wix-log-ingest",
            kwargs={"site_slug": self.slug, "secret": self.log_ingest_secret},
        )

    def wix_ingest_url(self) -> str:
        base = getattr(settings, "WEBSITE_LOGS_PUBLIC_BASE_URL", "").strip().rstrip("/")
        if not base:
            base = getattr(settings, "OIAT_PORTAL_BASE_URL", "").strip().rstrip("/")
        return f"{base}{self.wix_ingest_path()}" if base else self.wix_ingest_path()

    def last_log(self) -> WebsiteLogEvent | None:
        return self.log_events.order_by("-received_at", "-id").first()


class WebsiteLogEvent(models.Model):
    SEVERITY_DEBUG = "debug"
    SEVERITY_INFO = "info"
    SEVERITY_WARNING = "warning"
    SEVERITY_ERROR = "error"
    SEVERITY_CRITICAL = "critical"
    SEVERITY_UNKNOWN = "unknown"
    SEVERITY_CHOICES = [
        (SEVERITY_DEBUG, "Debug"),
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARNING, "Warning"),
        (SEVERITY_ERROR, "Error"),
        (SEVERITY_CRITICAL, "Critical"),
        (SEVERITY_UNKNOWN, "Unknown"),
    ]

    website = models.ForeignKey(Website, on_delete=models.CASCADE, related_name="log_events")
    received_at = models.DateTimeField(default=timezone.now)
    occurred_at = models.DateTimeField(null=True, blank=True)
    severity = models.CharField(max_length=16, choices=SEVERITY_CHOICES, default=SEVERITY_UNKNOWN)
    source = models.CharField(max_length=255, blank=True)
    event_type = models.CharField(max_length=255, blank=True)
    message = models.TextField(blank=True)
    request_id = models.CharField(max_length=255, blank=True)
    trace_id = models.CharField(max_length=255, blank=True)
    pathname = models.CharField(max_length=512, blank=True)
    function_name = models.CharField(max_length=255, blank=True)
    remote_addr = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    raw_payload = models.JSONField(default=dict, blank=True)
    request_headers = models.JSONField(default=dict, blank=True)
    context = models.JSONField(default=dict, blank=True)
    context_text = models.TextField(blank=True)

    class Meta:
        ordering = ["-received_at", "-id"]
        indexes = [
            models.Index(fields=["website", "-received_at"]),
            models.Index(fields=["website", "severity", "-received_at"]),
            models.Index(fields=["website", "event_type", "-received_at"]),
            models.Index(fields=["request_id"]),
        ]

    def __str__(self) -> str:
        label = self.message or self.event_type or self.source or "Website log"
        return f"{self.website.slug}: {self.severity} - {label[:80]}"
