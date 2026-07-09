from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="core-home"),
    path("coming-soon/<slug:feature>/", views.coming_soon, name="core-coming-soon"),
    path("account/", views.account_profile, name="core-account"),
    path(
        "account/change-password/",
        views.AccountPasswordChangeView.as_view(),
        name="core-password-change",
    ),
    path(
        "account/change-password/done/",
        views.AccountPasswordChangeDoneView.as_view(),
        name="core-password-change-done",
    ),
]
