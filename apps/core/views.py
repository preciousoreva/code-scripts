import os

from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import PasswordChangeDoneView, PasswordChangeView
from django.http import Http404
from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse, reverse_lazy

from apps.epos_qbo.models import CompanyConfigRecord
from apps.core.forms import AccountProfileForm


def logout_view(request):
    """Custom logout view that redirects to login page."""
    logout(request)
    return redirect("/login/")


@login_required
def home(request):
    """Show available solution workspaces."""
    solutions = []
    for item in getattr(settings, "PORTAL_SOLUTIONS", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        description = str(item.get("description") or "").strip()
        url_name = str(item.get("url_name") or "").strip()
        if not (name and description and url_name):
            continue
        try:
            href = reverse(url_name)
        except Exception:
            continue
        solutions.append({
            "name": name,
            "description": description,
            "href": href,
        })
    return render(request, "core/home.html", {"solutions": solutions})


@login_required
def coming_soon(request, feature: str):
    feature_titles = {
        "mappings": "Mappings",
        "settings": "Settings",
        "api-tokens": "API Tokens",
    }
    title = feature_titles.get(feature)
    if not title:
        raise Http404("Unknown feature")
    return render(request, "core/coming_soon.html", {"feature_title": title})


def _account_nav_context():
    """Context needed for sidebar when rendering account pages (same layout as epos_qbo)."""
    ui_debug = os.getenv("OIAT_UI_DEBUG_BEACON", "0").strip().lower() in {"1", "true", "yes", "on"}
    return {
        "company_count": CompanyConfigRecord.objects.filter(is_active=True).count(),
        "ui_debug_beacon_enabled": ui_debug,
    }


@login_required
def account_profile(request):
    """User account profile: edit display name/email, view permissions, change password."""
    user = request.user
    if request.method == "POST":
        form = AccountProfileForm(request.POST, instance=user)
        if form.is_valid():
            form.save()
            messages.success(request, "Profile updated.")
            return redirect("core-account")
    else:
        form = AccountProfileForm(instance=user)

    permissions = list(
        user.get_all_permissions()
        - {"auth.change_user", "auth.delete_user", "auth.view_user"}
    )
    portal_perms = [p for p in permissions if p.startswith("epos_qbo.")]
    context = {
        "profile_form": form,
        "portal_permissions": sorted(portal_perms),
        "is_superuser": user.is_superuser,
        "breadcrumbs": [
            {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
            {"label": "Account", "url": None},
        ],
    }
    context.update(_account_nav_context())
    return render(request, "core/account.html", context)


def _account_breadcrumbs(*, current_label: str, include_account: bool = True):
    """Breadcrumbs for account subpages (dashboard -> account [-> current])."""
    crumbs = [
        {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
    ]
    if include_account:
        crumbs.append({"label": "Account", "url": reverse("core-account")})
    crumbs.append({"label": current_label, "url": None})
    return crumbs


class AccountPasswordChangeView(PasswordChangeView):
    """Password change view that injects nav context for dashboard layout."""
    template_name = "registration/password_change_form.html"
    success_url = reverse_lazy("core-password-change-done")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_account_nav_context())
        context["breadcrumbs"] = _account_breadcrumbs(current_label="Change password")
        return context


class AccountPasswordChangeDoneView(PasswordChangeDoneView):
    """Password change done view that injects nav context for dashboard layout."""
    template_name = "registration/password_change_done.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_account_nav_context())
        context["breadcrumbs"] = _account_breadcrumbs(current_label="Password changed")
        return context
