from django.urls import path

from . import views

app_name = "epos_qbo"

urlpatterns = [
    path("", views.overview, name="overview-root"),
    path("dashboard/", views.overview, name="overview"),
    path("runs/", views.runs_list, name="runs"),
    path("runs/trigger", views.trigger_run, name="run-trigger"),
    path("runs/<uuid:job_id>/", views.run_detail, name="run-detail"),
    path("runs/<uuid:job_id>/logs", views.run_logs, name="run-logs"),
    path("companies/new", views.company_new, name="company-new"),
    path("companies/<slug:company_key>/advanced", views.company_advanced, name="company-advanced"),
    path("companies/<slug:company_key>/sync-json", views.sync_company_json, name="company-sync-json"),
]
