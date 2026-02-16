from django.urls import path

from . import views

app_name = "epos_qbo"

urlpatterns = [
    path("", views.overview, name="overview-root"),
    path("dashboard/", views.overview, name="overview"),
    path("dashboard/panels/", views.overview_panels, name="overview-panels"),
    path("runs/", views.runs_list, name="runs"),
    path("runs/trigger", views.trigger_run, name="run-trigger"),
    path("runs/<uuid:job_id>/", views.run_detail, name="run-detail"),
    path("runs/<uuid:job_id>/logs", views.run_logs, name="run-logs"),
    path("api/runs/active", views.run_active_ids, name="run-active-ids"),
    path("api/runs/status", views.run_status_check, name="run-status-check"),
    path("logs/", views.logs_list, name="logs"),
    path("companies/", views.companies_list, name="companies-list"),
    path("companies/new", views.company_new, name="company-new"),
    path("companies/<slug:company_key>/", views.company_detail, name="company-detail"),
    path("companies/<slug:company_key>/toggle-active/", views.company_toggle_active, name="company-toggle-active"),
    path("companies/<slug:company_key>/advanced", views.company_advanced, name="company-advanced"),
    path("companies/<slug:company_key>/sync-json", views.sync_company_json, name="company-sync-json"),
    path("settings/", views.settings_page, name="settings"),
]
