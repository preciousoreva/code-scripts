from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.urls import reverse


@login_required
def home(request):
    return render(
        request,
        "core/home.html",
        {
            "solutions": [
                {
                    "name": "EPOS -> QBO",
                    "description": "Monitor and operate EPOS to QuickBooks sales pipeline runs.",
                    "href": reverse("epos_qbo:overview"),
                }
            ]
        },
    )
