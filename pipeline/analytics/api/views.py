"""DRF endpoints + a minimal verification dashboard. All reads hit ClickHouse."""
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response

from . import queries


def _params(request):
    g = request.query_params
    return {
        "start": g.get("start") or None,
        "end": g.get("end") or None,
        "currency": g.get("currency") or None,
        "plan": g.get("plan") or None,
    }


@api_view(["GET"])
def daily(request):
    """GET /api/metrics/daily?start=&end=&currency=&plan="""
    p = _params(request)
    rows = queries.daily_metrics(**p)
    return Response({"filters": p, "count": len(rows), "results": rows})


@api_view(["GET"])
def summary(request):
    """GET /api/metrics/summary?start=&end=&currency=&plan="""
    p = _params(request)
    return Response({"filters": p, "summary": queries.summary_metrics(**p)})


@api_view(["GET"])
def cohorts(request):
    """GET /api/metrics/cohorts?plan="""
    plan = request.query_params.get("plan") or None
    return Response({"results": queries.cohort_retention(plan=plan)})


def dashboard(request):
    return render(request, "dashboard.html")
