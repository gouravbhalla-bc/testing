from typing import Any
from fastapi.responses import JSONResponse
from fastapi import Request
import structlog

logger = structlog.get_logger()


class DebouncerException(Exception):
    def __init__(self, id, ttl):
        self.id = id
        self.ttl = ttl


class ResourceNotFound(Exception):
    def __init__(self, name: str, id: Any):
        self.name = name
        self.id = id


class ResourceExists(Exception):
    def __init__(self, name: str, existingId: Any):
        self.name = name
        self.existingId = existingId


class ResourceNotUpdated(Exception):
    def __init__(self, name: str, id: Any, reason: str):
        self.name = name
        self.id = id
        self.reason = reason


class ResourceRateLimited(Exception):
    def __init__(self, name: str, id: Any, reason: str):
        self.name = name
        self.id = id
        self.reason = reason


class ExternalSystemError(Exception):
    def __init__(self, reason: str):
        self.reason = reason


class ResourceInvalidChain(Exception):
    def __init__(self, chain: str):
        self.chain = chain


def register_handlers(app):
    @app.exception_handler(ResourceNotFound)
    async def resource_not_found_handler(request: Request, exc: ResourceNotFound):
        msg = f"Oops, {exc.name} does not have a resource with id: {exc.id}".strip()
        logger.info(msg)
        return JSONResponse(
            status_code=404,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(DebouncerException)
    async def resource_debounced_handler(request: Request, exc: DebouncerException):
        msg = f"Oops, Too many requests for resource {exc.id}. Retry after {str(exc.ttl)}s".strip()
        logger.info(msg)
        return JSONResponse(
            status_code=429,
            content={"message": msg},
            headers={"x-error": msg, "retry-after": str(exc.ttl)},
        )

    @app.exception_handler(ResourceNotUpdated)
    async def resource_not_updated_handler(request: Request, exc: ResourceNotUpdated):
        msg = f"Oops, {exc.name} failed to update resource: {exc.id}. Reason: {exc.reason}".strip()
        logger.info(msg)

        return JSONResponse(
            status_code=400,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        msg = f"Oops, Invalid value given. Reason: {exc}".strip()
        return JSONResponse(
            status_code=400,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(ResourceInvalidChain)
    async def invalid_chain_handler(request: Request, exc: ResourceInvalidChain):
        msg = f"Oops, {exc.chain} is not a supported chain.".strip()
        return JSONResponse(
            status_code=400,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(ResourceExists)
    async def resource_exists_handler(request: Request, exc: ResourceExists):
        msg = f"Oops, {exc.name} has a conflict with an existing record (id:{exc.existingId})".strip()
        logger.info(msg)
        return JSONResponse(
            status_code=409,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(ResourceRateLimited)
    async def resource_rate_limited_handler(request: Request, exc: ResourceRateLimited):
        msg = f"Oops, we have hit a rate limiting error. {exc.reason} Id: {exc.id}, name: {exc.name}".strip()
        logger.info(msg)
        return JSONResponse(
            status_code=429,
            content={"message": msg},
            headers={"x-error": msg},
        )

    @app.exception_handler(ExternalSystemError)
    async def external_system_error_handler(request: Request, exc: ExternalSystemError):
        msg = f"Oops we have unexpected issue when calling external system. {exc.reason}".strip()
        logger.error(msg)
        return JSONResponse(
            status_code=500,
            content={"message": msg},
            headers={"x-error": msg},
        )
