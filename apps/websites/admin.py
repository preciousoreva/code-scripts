from django.contrib import admin

from .models import Website, WebsiteLogEvent


@admin.register(Website)
class WebsiteAdmin(admin.ModelAdmin):
    list_display = ("name", "domain", "platform", "is_active", "updated_at")
    list_filter = ("platform", "is_active")
    search_fields = ("name", "domain", "slug")
    readonly_fields = ("created_at", "updated_at")
    prepopulated_fields = {"slug": ("name",)}


@admin.register(WebsiteLogEvent)
class WebsiteLogEventAdmin(admin.ModelAdmin):
    list_display = ("website", "severity", "event_type", "source", "received_at")
    list_filter = ("severity", "event_type", "website")
    search_fields = ("message", "source", "request_id", "trace_id", "pathname", "function_name")
    readonly_fields = ("received_at",)
