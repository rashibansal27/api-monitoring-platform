"""
Root API router — mounts all sub-routers under /api.

Route map:
  /api/health/payment          GET  → health.py
  /api/health/payment/all      GET  → health.py
  /api/tps                     GET  → tps.py
  /api/latency                 GET  → latency.py
  /api/error/technical-rate    GET  → errors.py
  /api/error/business-rate     GET  → errors.py
  /api/anomaly/client/{id}     GET  → anomaly.py
  /api/anomaly/signals         GET  → anomaly.py
"""

from fastapi import APIRouter

from api.routes import anomaly, errors, health, latency, tps

router = APIRouter(prefix="/api")

router.include_router(health.router)
router.include_router(tps.router)
router.include_router(latency.router)
router.include_router(errors.router)
router.include_router(anomaly.router)
