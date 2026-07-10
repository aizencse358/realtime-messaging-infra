import logging
import os
import time

from fastapi import FastAPI, Request

from src.metrics import http_request_seconds

logger = logging.getLogger("gateway")


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def install_request_logging(app: FastAPI) -> None:
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration_seconds = time.perf_counter() - start
        duration_ms = duration_seconds * 1000
        path = request.scope.get("route").path if request.scope.get("route") else request.url.path
        http_request_seconds.labels(request.method, path, str(response.status_code)).observe(
            duration_seconds
        )
        logger.info(
            "event=request method=%s path=%s status=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response
