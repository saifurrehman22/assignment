from django.urls import path

from . import views

urlpatterns = [
    path("metrics/daily", views.daily, name="metrics-daily"),
    path("metrics/summary", views.summary, name="metrics-summary"),
    path("metrics/cohorts", views.cohorts, name="metrics-cohorts"),
]
