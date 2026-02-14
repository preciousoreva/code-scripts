from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="core-home"),
    path("coming-soon/<slug:feature>/", views.coming_soon, name="core-coming-soon"),
]
