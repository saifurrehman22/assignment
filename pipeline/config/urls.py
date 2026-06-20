from django.urls import include, path

from analytics.api import views as api_views

urlpatterns = [
    path("", api_views.dashboard, name="dashboard"),
    path("api/", include("analytics.api.urls")),
]
