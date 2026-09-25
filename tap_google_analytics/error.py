"""GoogleAnalytics error classes."""

from __future__ import annotations

import contextlib
import json
import logging
import socket

from google.api_core import exceptions as google_exceptions
from nekt_singer_sdk.custom_logger import internal_logger, user_logger


class TapGaApiError(Exception):
    """Base exception for API errors."""


class TapGaInvalidArgumentError(TapGaApiError):
    """Exception for errors on the report definition."""


class TapGaAuthenticationError(TapGaApiError):
    """Exception for UNAUTHENTICATED && PERMISSION_DENIED errors."""


class TapGaRateLimitError(TapGaApiError):
    """Exception for Rate Limit errors."""


class TapGaQuotaExceededError(TapGaApiError):
    """Exception for Quota Exceeded errors."""


class TapGaBackendServerError(TapGaApiError):
    """Exception for 500 and 503 backend errors that are Google's fault."""


class TapGaUnknownError(TapGaApiError):
    """Exception for unknown errors."""


NON_FATAL_ERRORS = [
    "userRateLimitExceeded",
    "rateLimitExceeded",
    "quotaExceeded",
    "internalServerError",
    "backendError",
]


def error_reason(e):
    """Return parsed reason from error message."""
    # For a given HttpError object from the googleapiclient package, this returns the
    # first reason code from
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors if the
    # errors HTTP response
    # body is valid json. Note that the code samples for Python on that page are
    # actually incorrect, and that
    # e.resp.reason is the HTTP transport level reason associated with the status code,
    # like "Too Many Requests"
    # for a 429 response code, whereas we want the reason field of the first error in
    # the JSON response body.

    reason = ""
    with contextlib.suppress(Exception):
        data = json.loads(e.content.decode("utf-8"))
        reason = data["error"]["errors"][0]["reason"]
    return reason


# Silence the discovery_cache errors
LOGGER = logging.getLogger("googleapiclient.discovery_cache")
LOGGER.setLevel(logging.ERROR)


# Errors the GA4 Data API client (google.api_core) raises for conditions that clear up on their
# own. They carry no `content`, so `error_reason` can never classify them.
TRANSIENT_GOOGLE_ERRORS = (
    google_exceptions.TooManyRequests,  # 429, includes ResourceExhausted (quota)
    google_exceptions.InternalServerError,  # 500
    google_exceptions.BadGateway,  # 502
    google_exceptions.ServiceUnavailable,  # 503
    google_exceptions.GatewayTimeout,  # 504, includes DeadlineExceeded
)


def is_quota_error(error) -> bool:
    """Return True for a 429 (GA4 quota or rate limit)."""
    return isinstance(error, google_exceptions.TooManyRequests)


def quota_window(error) -> str | None:
    """Return "hour" or "day" when a 429 names which GA4 token bucket ran out, else None.

    GA4 words it as e.g. "Exhausted property tokens for a project per hour. These quota tokens
    will return in under an hour." Other 429s (concurrent requests) name no window.
    """
    message = str(getattr(error, "message", None) or error).lower()
    if "per day" in message:
        return "day"
    if "per hour" in message:
        return "hour"
    return None


def is_timeout_error(error) -> bool:
    """Return True when the request ran out of time (504 / DeadlineExceeded)."""
    return isinstance(error, google_exceptions.GatewayTimeout)


def is_fatal_error(error):
    """Return a boolean value depending on if its a fatal error or not."""
    if isinstance(error, (socket.timeout, *TRANSIENT_GOOGLE_ERRORS)):
        return False

    try:
        status = error.code if error.message is not None else None
    except:
        status = None

    if status in [500, 503]:
        return False

    # Use list of errors defined in:
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    reason = error_reason(error)
    if reason in NON_FATAL_ERRORS:
        return False

    user_logger.error(
        f"Google Analytics rejected the request: {getattr(error, 'message', None) or error}"
    )
    internal_logger.error(f"Received fatal error {error!r}, reason={reason}, status={status}")
    return True
