from django.urls import path

from . import views, webhooks

app_name = "websites"

urlpatterns = [
    path("", views.index, name="index"),
    path("sites/<slug:site_slug>/", views.website_detail, name="detail"),
    path("sites/<slug:site_slug>/logs/", views.website_logs, name="logs"),
    path("sites/<slug:site_slug>/api/logs/", views.website_logs_api, name="logs-api"),
    path("sites/<slug:site_slug>/api/logs/<int:log_id>/", views.website_log_detail_api, name="log-detail-api"),
    path(
        "webhooks/wix/<slug:site_slug>/<str:secret>/",
        webhooks.wix_log_ingest,
        name="wix-log-ingest",
    ),
]
