from __future__ import annotations

from django.http import HttpResponse
from django.urls import path

urlpatterns: list = [
    # no web endpoints yet (ORM-only use cases supported)
    path("healthz/", lambda _req: HttpResponse("ok")),
]
