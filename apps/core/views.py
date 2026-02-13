from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.urls import reverse


def logout_view(request):
    """Custom logout view that redirects to login page."""
    logout(request)
    return redirect("/login/")


@login_required
def home(request):
    """Redirect logged-in users to the dashboard."""
    return redirect("epos_qbo:overview")
