"""Custom client handling, including GoogleAnalyticsStream base class."""

from __future__ import annotations

import copy
import functools
import sys
import time
import typing as t
from datetime import date, datetime, timedelta, timezone

import pendulum
from google.analytics.data_v1beta.types import (
    DateRange,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.streams import REPLICATION_FULL_TABLE, Stream

from tap_google_analytics.error import (
    is_fatal_error,
    is_quota_error,
    is_timeout_error,
    quota_window,
)

# Seconds to wait before each retry. Server errors and timeouts clear up in seconds (same
# 5 attempts as the previous backoff). 429s that name no bucket (e.g. concurrent requests)
# wait ~15 min in total.
TRANSIENT_RETRY_WAITS = (5, 15, 45, 90)
QUOTA_RETRY_WAITS = (60, 120, 240, 480)

# An exhausted hourly token bucket "returns in under an hour": poll every 5 min for up to 65 min.
HOURLY_QUOTA_POLL_SECONDS = 300
HOURLY_QUOTA_MAX_WAIT_SECONDS = 65 * 60
# Total time a run may spend waiting on quota, across all streams. The platform stops a run
# after 5h45, so a backfill that needs more hours of quota than this fails instead of being killed.
QUOTA_WAIT_BUDGET_SECONDS = 4 * 60 * 60
_quota_wait = {"spent": 0}

# Reports with the `date` dimension fetch longer ranges one calendar month at a time: with offset
# pagination each 100k-row page re-runs the report over the whole range, which is what burns the
# token budget on backfills. Incremental runs (1-2 days) stay a single request.
WINDOW_THRESHOLD_DAYS = 31

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()


class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        self.analytics = kwargs.pop("ga_analytics_client")

        super().__init__(*args, **kwargs)

        self.end_date = self._get_end_date()
        self.property_id = self.config["property_id"]
        self.page_size = 100000
        self._split_warned = False

    def _get_end_date(self):
        end_date_config = self.config.get("end_date")
        end_date = (
            datetime.strptime(end_date_config, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date_config
            else datetime.now(timezone.utc)
        )
        end_date_offset = end_date - timedelta(days=1)

        return end_date_offset.strftime("%Y-%m-%d")

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        internal_logger.error(f"Unsupported GA type: {type}")
        sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if (
            (attribute.startswith("goal"))
            and attribute.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            )
        ) or attribute.startswith(("metric", "calcMetric")):
            return "string"

        if attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])

        internal_logger.error(f"Unsupported GA type: {type}")
        sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type in ["integer"]:
            data_type = "integer"
        elif attr_type in ["float", "percent", "time", "seconds"]:
            data_type = "number"

        return data_type

    def _lookup_data_type(self, field_type, attribute, dimensions_ref, metrics_ref):
        """Get the data type of a metric or a dimension."""
        if field_type == "dimension":
            return self._parse_dimension_type(attribute, dimensions_ref)

        if field_type == "metric":
            return self._parse_metric_type(attribute, metrics_ref)

        internal_logger.error(f"Unsupported GA type: {field_type}")
        sys.exit(1)

    @staticmethod
    def _generate_report_definition(report_def_raw):
        report_definition = {
            "metrics": [],
            "dimensions": [],
            "metricFilter": None,
            "dimensionFilter": None,
        }

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append({"name": dimension})

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(Metric(name=metric))

        if "metricFilter" in report_def_raw:
            report_definition["metricFilter"] = report_def_raw["metricFilter"]

        if "dimensionFilter" in report_def_raw:
            report_definition["dimensionFilter"] = report_def_raw["dimensionFilter"]

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = [
                {"segmentId": segment_id} for segment_id in report_def_raw["segments"]
            ]
        return report_definition

    def _request_data(
        self,
        api_report_def,
        state_filter: str,
        next_page_token: t.Any | None,
        *,
        end_date: str | None = None,
        retry_timeouts: bool = True,
    ) -> RunReportResponse:
        return self._query_api(
            api_report_def,
            state_filter,
            next_page_token,
            end_date=end_date,
            retry_timeouts=retry_timeouts,
        )

    def _can_split_range(self, start_date: str, end_date: str) -> bool:
        """Return True when a timed-out range may be fetched as two halves instead.

        Only reports with the ``date`` dimension: each row is already one day, so two halves
        return exactly the same rows. Without it GA4 aggregates over the whole range (active
        users of two halves do not add up), and splitting would change the numbers.
        """
        if "date" not in self.report["dimensions"]:
            return False
        return date.fromisoformat(start_date) < date.fromisoformat(end_date)

    def _get_state_filter(self, context: Context | None) -> str:
        if self.replication_method == REPLICATION_FULL_TABLE:
            start_date = pendulum.parse(self.config["start_date"]).date()
        else:
            start_date = pendulum.parse(
                self.get_context_state(context).get(
                    "replication_key_value", self.config["start_date"]
                )
            ).date()
        parsed = max(start_date, date(2019, 1, 1))
        # state bookmarks need to be reformatted for API requests
        user_logger.info(f"[{self.name}] Starting sync from {parsed}.")
        return date.strftime(parsed, "%Y-%m-%d")

    def _request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        state_filter = self._get_state_filter(context)
        api_report_def = self._generate_report_definition(self.report)
        windows = self._sync_windows(state_filter, self.end_date)
        if len(windows) > 1:
            internal_logger.info(
                f"[{self.name}] {state_filter}..{self.end_date} is fetched in {len(windows)} "
                "monthly windows"
            )
        for start_date, end_date in windows:
            yield from self._request_range(api_report_def, start_date, end_date)

    def _sync_windows(self, start_date: str, end_date: str) -> list[tuple[str, str]]:
        """Return the date ranges to request: calendar months for long ranges of date reports.

        Each row of a report with the `date` dimension is one day, so the rows are the same
        whether the range is requested at once or month by month. Other reports aggregate over
        the range and are always requested whole.
        """
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
        if "date" not in self.report["dimensions"] or (end - start).days < WINDOW_THRESHOLD_DAYS:
            return [(start_date, end_date)]

        windows = []
        while start <= end:
            next_month = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
            window_end = min(next_month - timedelta(days=1), end)
            windows.append((start.isoformat(), window_end.isoformat()))
            start = next_month
        return windows

    def _request_range(self, api_report_def, start_date: str, end_date: str) -> t.Iterable[dict]:
        """Page through one date range, halving it when Google times out on it.

        A range that succeeds is fetched exactly as before (one report, offset pagination), so
        the split only ever happens on the path that used to kill the run.
        """
        next_page_token: t.Any = None
        finished = False
        splittable = self._can_split_range(start_date, end_date)

        while not finished:
            try:
                resp = self._request_data(
                    api_report_def,
                    state_filter=start_date,
                    next_page_token=next_page_token,
                    end_date=end_date,
                    retry_timeouts=not splittable,
                )
            except Exception as error:
                if not (splittable and is_timeout_error(error)):
                    raise
                first_end, second_start = self._split_range(start_date, end_date)
                if not self._split_warned:
                    self._split_warned = True
                    user_logger.warning(
                        f"[{self.name}] Google Analytics took too long to answer for the period "
                        f"{start_date} to {end_date}. It is now being fetched in smaller parts; "
                        "the run takes longer, but no data is skipped."
                    )
                internal_logger.warning(
                    f"[{self.name}] timeout on {start_date}..{end_date} "
                    f"page={next_page_token or 0} ({error!r}); splitting into "
                    f"{start_date}..{first_end} and {second_start}..{end_date}. Rows of pages "
                    "already emitted are re-emitted (same PK, deduplicated by the target)."
                )
                yield from self._request_range(api_report_def, start_date, first_end)
                yield from self._request_range(api_report_def, second_start, end_date)
                return

            yield from self._parse_response(resp)

            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self._get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                msg = (
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
                raise RuntimeError(msg)
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def _get_next_page_token(self, response: RunReportResponse, previous_token) -> t.Any:
        """Get the next page token from a response.

        Args:
            response: The response from the API.
            previous_token: The previous page token.

        Returns:
            The next page token, or None if there are no more pages.
        """
        previous_token = previous_token or 0
        next_token = previous_token + 1
        total_rows = response.row_count
        return next_token if total_rows >= next_token * self.page_size else None

    def _sanitize_custom_dimension(self, dimension: str) -> str:
        return dimension.replace("customEvent:", "custom_event_").replace(
            "customUser:", "custom_user_"
        )

    def _parse_response(self, response):
        if not response:
            return
        dimensionHeaders = [d.name for d in response.dimension_headers]  # noqa: N806
        metricHeaders = [mh.name for mh in response.metric_headers]  # noqa: N806

        for row in response.rows:
            record = {}
            dimensions = [d.value for d in row.dimension_values]
            dateRangeValues = row.metric_values  # noqa: N806

            for header, dimension in zip(dimensionHeaders, dimensions):
                data_type = self._lookup_data_type(
                    "dimension", header, self.dimensions_ref, self.metrics_ref
                )

                if data_type == "integer":
                    value = int(dimension)
                elif data_type == "number":
                    value = float(dimension)
                else:
                    value = dimension

                record[self._sanitize_custom_dimension(header)] = value

            for metric_name, value in zip(metricHeaders, dateRangeValues):
                metric_type = self._lookup_data_type(
                    "metric", metric_name, self.dimensions_ref, self.metrics_ref
                )

                if hasattr(value, "value"):
                    value = value.value  # noqa: PLW2901

                if metric_type == "integer":
                    value = int(value)  # noqa: PLW2901
                elif metric_type == "number":
                    value = float(value)  # noqa: PLW2901

                record[metric_name] = value

            # Also add the [start_date,end_date) used for the report
            record["report_start_date"] = self.config.get("start_date")
            record["report_end_date"] = self.end_date

            yield record

    @staticmethod
    def _split_range(start_date: str, end_date: str) -> tuple[str, str]:
        """Return (end of the first half, start of the second half) of an inclusive range."""
        start = date.fromisoformat(start_date)
        days = (date.fromisoformat(end_date) - start).days + 1
        first_end = start + timedelta(days=days // 2 - 1)
        return first_end.isoformat(), (first_end + timedelta(days=1)).isoformat()

    def _query_api(
        self,
        report_definition,
        state_filter,
        pageToken=None,  # noqa: N803
        *,
        end_date: str | None = None,
        retry_timeouts: bool = True,
    ) -> RunReportResponse:
        """Run one GA4 report page, waiting out transient errors.

        An exhausted hourly token bucket is polled until it refills (up to about an hour, within
        the run's quota-wait budget); an exhausted daily bucket fails at once; other 429s wait
        minutes and other transient errors wait seconds. With ``retry_timeouts=False`` a timeout
        is raised at once so the caller can split the date range instead of repeating a request
        that cannot fit.

        Returns:
            The GA4 Data API response.
        """
        end_date = end_date or self.end_date
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=report_definition["dimensions"],
            metrics=report_definition["metrics"],
            date_ranges=[DateRange(start_date=state_filter, end_date=end_date)],
            limit=self.page_size,
            metric_filter=report_definition["metricFilter"],
            dimension_filter=report_definition["dimensionFilter"],
            offset=(pageToken or 0) * self.page_size,
            return_property_quota=True,
        )

        transient_waits = iter(TRANSIENT_RETRY_WAITS)
        quota_waits = iter(QUOTA_RETRY_WAITS)
        hourly_waited = 0
        attempt = 0
        while True:
            attempt += 1
            where = f"{state_filter}..{end_date} page={pageToken or 0} attempt={attempt}"
            try:
                response = self.analytics.run_report(request)
            except Exception as error:
                if (not retry_timeouts and is_timeout_error(error)) or is_fatal_error(error):
                    raise
                quota = is_quota_error(error)
                window = quota_window(error) if quota else None
                if window == "day":
                    user_logger.error(
                        f"[{self.name}] The daily Google Analytics API quota for this property is "
                        "exhausted. It refills at midnight Pacific Time; run the source again "
                        "after that, or sync a shorter period (a more recent start date)."
                    )
                    internal_logger.error(f"[{self.name}] daily quota on {where}: {error!r}")
                    raise
                if window == "hour":
                    wait = self._hourly_quota_wait(hourly_waited, where, error)
                    if wait is None:
                        raise
                    hourly_waited += wait
                    time.sleep(wait)
                    continue
                wait = next(quota_waits if quota else transient_waits, None)
                if wait is None:
                    if quota:
                        user_logger.error(
                            f"[{self.name}] The Google Analytics API quota for this property is "
                            "exhausted and did not recover after waiting about 15 minutes. "
                            "Try again later, or sync a shorter period (a more recent start date)."
                        )
                    internal_logger.error(
                        f"[{self.name}] giving up on {where}: {error!r}", exc_info=True
                    )
                    raise
                if quota:
                    user_logger.warning(
                        f"[{self.name}] Google Analytics quota reached; waiting {wait // 60} "
                        "minute(s) before trying again."
                    )
                internal_logger.warning(
                    f"[{self.name}] transient error on {where}: {error!r}; retrying in {wait}s"
                )
                time.sleep(wait)
                continue

            self._log_property_quota(response, where)
            return response

    def _hourly_quota_wait(self, hourly_waited: int, where: str, error) -> int | None:
        """Return the seconds to wait for an exhausted hourly token bucket, or None to give up.

        Gives up once this request has waited about an hour (the bucket should have refilled)
        or once the run has used its whole quota-wait budget.
        """
        budget_left = QUOTA_WAIT_BUDGET_SECONDS - _quota_wait["spent"]
        if hourly_waited >= HOURLY_QUOTA_MAX_WAIT_SECONDS or budget_left <= 0:
            user_logger.error(
                f"[{self.name}] The hourly Google Analytics API quota for this property did not "
                f"refill in time (this run waited {_quota_wait['spent'] // 60} minutes for quota "
                "in total). Try again later, or sync a shorter period (a more recent start date)."
            )
            internal_logger.error(
                f"[{self.name}] giving up on {where} after {hourly_waited}s on this request, "
                f"{_quota_wait['spent']}s of quota waits in the run: {error!r}"
            )
            return None

        wait = min(HOURLY_QUOTA_POLL_SECONDS, budget_left)
        if hourly_waited == 0:
            user_logger.warning(
                f"[{self.name}] The hourly Google Analytics quota for this property is exhausted. "
                "Waiting up to an hour for it to refill; the run takes longer, but no data is "
                "skipped."
            )
        _quota_wait["spent"] += wait
        internal_logger.warning(
            f"[{self.name}] hourly quota on {where}: {error!r}; waited {hourly_waited}s on this "
            f"request, {_quota_wait['spent']}s in the run; retrying in {wait}s"
        )
        return wait

    def _log_property_quota(self, response, where: str) -> None:
        """Log the tokens GA4 reports for this property, to measure what each request costs."""
        quota = getattr(response, "property_quota", None)
        if not quota:
            return
        buckets = (
            "tokens_per_project_per_hour",
            "tokens_per_hour",
            "tokens_per_day",
            "concurrent_requests",
        )
        usage = ", ".join(
            f"{name}={getattr(quota, name).consumed}/{getattr(quota, name).remaining}"
            for name in buckets
            if getattr(quota, name, None)
        )
        internal_logger.info(
            f"[{self.name}] {where} rows={getattr(response, 'row_count', '?')} "
            f"quota consumed/remaining: {usage}"
        )

    @staticmethod
    def _get_datatype(string_type):
        mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "number": th.NumberType(),
        }
        return mapping.get(string_type, th.StringType())

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from self._request_records(context)

    @functools.cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: list[th.Property] = []
        primary_keys = []
        # : List[th.StringType] = []

        # Track if there is a date set as one of the Dimensions
        date_dimension_included = False

        # Add the dimensions to the schema and as key_properties
        for dimension in self.report["dimensions"]:
            if dimension == "date":
                date_dimension_included = True
                self.replication_key = "date"
            data_type = self._lookup_data_type(
                "dimension", dimension, self.dimensions_ref, self.metrics_ref
            )
            dimension_sanitized = self._sanitize_custom_dimension(dimension)
            properties.append(
                th.Property(dimension_sanitized, self._get_datatype(data_type), required=True)
            )
            primary_keys.append(dimension_sanitized)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            properties.append(th.Property(metric, self._get_datatype(data_type)))

        properties.extend(
            (
                th.Property("report_start_date", th.StringType(), required=True),
                th.Property("report_end_date", th.StringType(), required=True),
            )
        )
        # If 'ga:date' has not been added as a Dimension, add the
        #  {start_date, end_date} params as keys
        if not date_dimension_included:
            user_logger.warning(
                "Incremental sync not supported for stream %s, 'ga.date' is the only "
                "supported replication key at this time.",
                self.tap_stream_id,
            )
            primary_keys.extend(("report_start_date", "report_end_date"))
        self.primary_keys = primary_keys
        return th.PropertiesList(*properties).to_dict()
