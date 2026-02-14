from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import redirect, render
from django.urls import reverse


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
