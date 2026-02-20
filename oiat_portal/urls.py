from django.contrib import admin
from django.contrib.auth import views as auth_views
from django.urls import include, path
from apps.core import views as core_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("login/", auth_views.LoginView.as_view(template_name="registration/login.html"), name="login"),
    path("logout/", core_views.logout_view, name="logout"),
    path("", include("apps.core.urls")),
    path("epos-qbo/", include("apps.epos_qbo.urls")),
]
