from django.urls import path

from . import views

app_name = "epos_qbo"

urlpatterns = [
    path("", views.overview, name="overview-root"),
    path("dashboard/", views.overview, name="overview"),
    path("dashboard/panels/", views.overview_panels, name="overview-panels"),
    path("runs/", views.runs_list, name="runs"),
    path("runs/trigger", views.trigger_run, name="run-trigger"),
    path("runs/trigger-inventory", views.trigger_inventory_run, name="run-trigger-inventory"),
    path("runs/<uuid:job_id>/", views.run_detail, name="run-detail"),
    path(
        "runs/<uuid:job_id>/artifacts/<int:artifact_id>/report/<slug:report_key>/",
        views.run_artifact_report,
        name="run-artifact-report",
    ),
    path("runs/<uuid:job_id>/logs", views.run_logs, name="run-logs"),
    path("api/runs/active", views.run_active_ids, name="run-active-ids"),
    path("api/runs/status", views.run_status_check, name="run-status-check"),
    path("logs/", views.logs_list, name="logs"),
    path("schedules/", views.schedules_page, name="schedules"),
    path("schedules/status/", views.schedule_status_api, name="schedule-status"),
    path("schedules/create", views.schedule_create, name="schedule-create"),
    path("schedules/<uuid:schedule_id>/update", views.schedule_update, name="schedule-update"),
    path("schedules/<uuid:schedule_id>/toggle", views.schedule_toggle, name="schedule-toggle"),
    path("schedules/<uuid:schedule_id>/run-now", views.schedule_run_now, name="schedule-run-now"),
    path("schedules/<uuid:schedule_id>/delete", views.schedule_delete, name="schedule-delete"),
    path("companies/", views.companies_list, name="companies-list"),
    path("companies/new", views.company_new, name="company-new"),
    path(
        "companies/<slug:company_key>/inventory/review/",
        views.company_inventory_review,
        name="company_inventory_review",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/mark-reviewed/",
        views.company_inventory_review_mark_reviewed,
        name="company_inventory_review_mark_reviewed",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/retry-catalog-cleanup/",
        views.company_inventory_retry_catalog_cleanup,
        name="company_inventory_retry_catalog_cleanup",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/retry-catalog-cleanup/confirm/",
        views.company_inventory_retry_catalog_cleanup_confirm,
        name="company_inventory_retry_catalog_cleanup_confirm",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/retry-quantity-adjustments/",
        views.company_inventory_retry_quantity_adjustments,
        name="company_inventory_retry_quantity_adjustments",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/retry-quantity-adjustments/confirm/",
        views.company_inventory_retry_quantity_adjustments_confirm,
        name="company_inventory_retry_quantity_adjustments_confirm",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/missing-preview/",
        views.company_inventory_missing_preview,
        name="company_inventory_missing_preview",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/missing-create/confirm/",
        views.company_inventory_missing_create_confirm,
        name="company_inventory_missing_create_confirm",
    ),
    path(
        "companies/<slug:company_key>/inventory/review/missing-create/",
        views.company_inventory_missing_create,
        name="company_inventory_missing_create",
    ),
    path("companies/<slug:company_key>/", views.company_detail, name="company-detail"),
    path("companies/<slug:company_key>/toggle-active/", views.company_toggle_active, name="company-toggle-active"),
    path("companies/<slug:company_key>/advanced", views.company_advanced, name="company-advanced"),
    path("companies/<slug:company_key>/sync-json", views.sync_company_json, name="company-sync-json"),
    path("settings/", views.settings_page, name="settings"),
    path("api-tokens/", views.api_tokens_page, name="api-tokens"),
    path("api-tokens/<slug:company_key>/test/", views.api_tokens_test, name="api-tokens-test"),
    path("api-tokens/<slug:company_key>/refresh/", views.api_tokens_refresh, name="api-tokens-refresh"),
    path("tools/", views.tools_page, name="tools"),
    path("tools/qbo-query/", views.tools_qbo_query_api, name="tools-qbo-query"),
    path("tools/verify-mapping/", views.tools_verify_mapping_api, name="tools-verify-mapping"),
]
