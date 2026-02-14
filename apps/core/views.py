from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.urls import reverse


def logout_view(request):
    """Custom logout view that redirects to login page."""
    logout(request)
    return redirect("/login/")


@login_required
def home(request):
    """Show available solution workspaces."""
    solutions = [
        {
            "name": "EPOS -> QBO",
            "description": "Monitor runs, manage companies, and trigger sync jobs.",
            "href": reverse("epos_qbo:overview"),
        }
    ]
    return render(request, "core/home.html", {"solutions": solutions})
