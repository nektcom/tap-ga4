# ruff: noqa: D102, D103, D107, PLR2004, SLF001
"""Retry and date-range split behaviour of GoogleAnalyticsStream, against a fake GA4 client."""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as google_exceptions

from tap_google_analytics import client as client_module
from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.error import is_fatal_error


def _response(days: list[str]):
    rows = [
        SimpleNamespace(
            dimension_values=[SimpleNamespace(value=day.replace("-", ""))],
            metric_values=[SimpleNamespace(value="1")],
        )
        for day in days
    ]
    return SimpleNamespace(
        dimension_headers=[SimpleNamespace(name="date")],
        metric_headers=[SimpleNamespace(name="sessions")],
        rows=rows,
        row_count=len(rows),
    )


def _days(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


class FakeAnalytics:
    """Answer run_report with one row per day; fail according to ``fail``."""

    def __init__(self, fail=None):
        self.fail = fail or (lambda _start, _end, _call: None)
        self.calls: list[tuple[str, str]] = []

    def run_report(self, request):
        date_range = request.date_ranges[0]
        start, end = date_range.start_date, date_range.end_date
        self.calls.append((start, end))
        error = self.fail(start, end, len(self.calls))
        if error is not None:
            raise error
        return _response(_days(start, end))


def _stream(analytics, dimensions=("date",)):
    stream = object.__new__(GoogleAnalyticsStream)
    stream.report = {"name": "pages", "dimensions": list(dimensions), "metrics": ["sessions"]}
    stream.dimensions_ref = {"date": "string"}
    stream.metrics_ref = {"sessions": "integer"}
    stream.analytics = analytics
    stream.property_id = "1"
    stream.page_size = 100000
    stream.end_date = "2024-01-08"
    stream._split_warned = False
    stream._config = {"start_date": "2024-01-01"}
    stream.name = "pages"
    return stream


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    sleeps: list[int] = []
    monkeypatch.setattr(client_module.time, "sleep", sleeps.append)
    return sleeps


def _records(stream):
    definition = stream._generate_report_definition(stream.report)
    return list(stream._request_range(definition, "2024-01-01", stream.end_date))


def test_success_is_a_single_request():
    analytics = FakeAnalytics()
    records = _records(_stream(analytics))

    assert analytics.calls == [("2024-01-01", "2024-01-08")]
    assert [r["date"] for r in records] == [
        d.replace("-", "") for d in _days("2024-01-01", "2024-01-08")
    ]


def test_timeout_splits_the_range_until_it_fits():
    def fail(start, end, _call):
        too_big = (date.fromisoformat(end) - date.fromisoformat(start)).days >= 2
        return google_exceptions.DeadlineExceeded("Deadline Exceeded") if too_big else None

    analytics = FakeAnalytics(fail)
    records = _records(_stream(analytics))

    assert sorted({r["date"] for r in records}) == [
        d.replace("-", "") for d in _days("2024-01-01", "2024-01-08")
    ]
    assert len(records) == 8
    served = [
        c for c in analytics.calls if (date.fromisoformat(c[1]) - date.fromisoformat(c[0])).days < 2
    ]
    assert served == [
        ("2024-01-01", "2024-01-02"),
        ("2024-01-03", "2024-01-04"),
        ("2024-01-05", "2024-01-06"),
        ("2024-01-07", "2024-01-08"),
    ]


def test_timeout_without_date_dimension_retries_instead_of_splitting(no_sleep):
    def fail(_start, _end, call):
        return google_exceptions.DeadlineExceeded("Deadline Exceeded") if call < 3 else None

    analytics = FakeAnalytics(fail)
    stream = _stream(analytics, dimensions=("pagePath",))
    stream.dimensions_ref = {"date": "string", "pagePath": "string"}
    definition = stream._generate_report_definition(stream.report)
    list(stream._request_range(definition, "2024-01-01", stream.end_date))

    assert analytics.calls == [("2024-01-01", "2024-01-08")] * 3
    assert no_sleep == [5, 15]


def test_single_day_timeout_is_retried_then_raised(no_sleep):
    analytics = FakeAnalytics(lambda *_: google_exceptions.DeadlineExceeded("Deadline Exceeded"))
    stream = _stream(analytics)
    definition = stream._generate_report_definition(stream.report)

    with pytest.raises(google_exceptions.DeadlineExceeded):
        list(stream._request_range(definition, "2024-01-01", "2024-01-01"))
    assert len(analytics.calls) == 5
    assert no_sleep == [5, 15, 45, 90]


def test_quota_waits_minutes_then_recovers(no_sleep):
    def fail(_start, _end, call):
        return (
            google_exceptions.ResourceExhausted("Exhausted property tokens per hour")
            if call < 3
            else None
        )

    analytics = FakeAnalytics(fail)
    records = _records(_stream(analytics))

    assert len(records) == 8
    assert no_sleep == [60, 120]


def test_quota_that_never_recovers_raises(no_sleep):
    analytics = FakeAnalytics(lambda *_: google_exceptions.ResourceExhausted("quota"))

    with pytest.raises(google_exceptions.ResourceExhausted):
        _records(_stream(analytics))
    assert no_sleep == [60, 120, 240, 480]


def test_client_errors_stay_fatal(no_sleep):
    analytics = FakeAnalytics(lambda *_: google_exceptions.PermissionDenied("no access"))

    with pytest.raises(google_exceptions.PermissionDenied):
        _records(_stream(analytics))
    assert len(analytics.calls) == 1
    assert no_sleep == []


@pytest.mark.parametrize(
    ("error", "fatal"),
    [
        (google_exceptions.DeadlineExceeded("x"), False),
        (google_exceptions.ResourceExhausted("x"), False),
        (google_exceptions.ServiceUnavailable("x"), False),
        (google_exceptions.InternalServerError("x"), False),
        (google_exceptions.InvalidArgument("x"), True),
        (google_exceptions.PermissionDenied("x"), True),
    ],
)
def test_is_fatal_error_classifies_google_api_errors(error, fatal):
    assert is_fatal_error(error) is fatal
