from datetime import date, datetime
from typing import Optional
import time

import requests

from pv_prospect.common import PVSite
from pv_prospect.data_extraction.extractors import ExtractionResult
from pv_prospect.data_extraction.util import map_from_env, VarMapping


URL = "https://pvoutput.org/service/r2/getstatus.jsp"
API_KEY_HEADER_NAME = "X-Pvoutput-Apikey"
OWN_SYSTEM_ID_HEADER_NAME = "X-Pvoutput-SystemId"

# Response headers from PVOutput API
RATE_LIMIT_HEADER = "X-Rate-Limit-Limit"
RATE_LIMIT_REMAINING_HEADER = "X-Rate-Limit-Remaining"
RATE_LIMIT_RESET_HEADER = "X-Rate-Limit-Reset"


CONSTANT_QUERY_PARAMS = {
    'h': 1,         # Get full status (see FIELDS)
    'limit': 288,   # Get all statuses for the day
    'asc': 1,       # Ascending order
}


HEADER = [
    'date',
    'time',
    'energy',
    'efficiency',
    'power',
    'average',
    'normalised',
    'energy_used',
    'power_used',
    'temperature',
    'voltage'
]


class PVOutputRateLimiter:
    """
    Rate limiter that uses PVOutput API response headers to track rate limits.
    Note: This is designed for use across multiple workers, so we only track
    the reset_time (which is shared across all workers) and not remaining_requests
    (which would be unreliable since each worker maintains its own instance).
    """

    def __init__(self):
        self.rate_limit: Optional[int] = None
        self.reset_time: Optional[datetime] = None
        self._first_update = True

    def update_from_headers(self, headers: dict[str, str]):
        """
        Update rate limit info from API response headers.

        Args:
            headers: Response headers from PVOutput API
        """
        if RATE_LIMIT_HEADER in headers:
            self.rate_limit = int(headers[RATE_LIMIT_HEADER])

        if RATE_LIMIT_RESET_HEADER in headers:
            # The reset time is in Unix timestamp (seconds since epoch)
            reset_timestamp = int(headers[RATE_LIMIT_RESET_HEADER])
            self.reset_time = datetime.fromtimestamp(reset_timestamp)

        # Print rate limit info on first update
        if self._first_update and self.rate_limit is not None:
            reset_time_str = self.reset_time.strftime('%H:%M:%S') if self.reset_time else 'unknown'
            print(f"ℹ️  Rate limit: {self.rate_limit} requests per hour (resets at {reset_time_str})")
            self._first_update = False

    def wait_if_needed(self):
        """
        Check if we need to wait for rate limit to reset.
        This should be called when a rate limit error (HTTP 403) is encountered.
        """
        if self.reset_time:
            current_time = datetime.now()
            if current_time < self.reset_time:
                sleep_seconds = (self.reset_time - current_time).total_seconds()
                if sleep_seconds > 0:
                    limit_info = f" (limit: {self.rate_limit})" if self.rate_limit else ""
                    print(f"\n⚠️  Rate limit reached{limit_info}.")
                    print(f"   Sleeping for {sleep_seconds:.1f} seconds until {self.reset_time.strftime('%H:%M:%S')}...\n")
                    time.sleep(sleep_seconds)
                    # Reset after sleeping
                    self.reset_time = None


class PVOutputExtractor:
    def __init__(self, api_key: str, system_id: str, rate_limiter: Optional[PVOutputRateLimiter] = None) -> None:
        self.api_key = api_key
        self.system_id = system_id
        self.rate_limiter = rate_limiter if rate_limiter else PVOutputRateLimiter()

    @classmethod
    def from_env(cls) -> 'PVOutputExtractor':
        rate_limiter = PVOutputRateLimiter()
        extractor = map_from_env(PVOutputExtractor, {
            'api_key': VarMapping('PVO_API_KEY', str),
            'system_id': VarMapping('PVO_SYSTEM_ID', str)
        }, rate_limiter=rate_limiter)
        return extractor

    def extract(self, pv_site: PVSite, date_: date, end_date: date = None) -> ExtractionResult:
        """
        Extract PV output status data for a single date from the PVOutput API for the given site.

        Args:
            pv_site: PVSite object containing the target system id and site metadata.
            date_: The date to extract (only a single date is supported by the API).
            end_date: Optional end date (ignored by this extractor since PVOutput only supports single-date queries).

        Returns:
            ExtractionResult: Object containing extracted CSV-style rows in `data` and optional `metadata`.

        Raises:
            ValueError: If pv_site is missing or does not contain a valid system id.
            requests.HTTPError: If the HTTP request to the PVOutput API fails.
        """
        if not pv_site or not pv_site.pvo_sys_id:
            raise ValueError("PVSite must be provided with a valid system ID")

        # PVOutput API only supports single-date queries, so ignore end_date
        # If multi-date support is needed in the future, multiple API calls would be required

        headers = {
            API_KEY_HEADER_NAME: self.api_key,
            OWN_SYSTEM_ID_HEADER_NAME: self.system_id,
            'X-Rate-Limit': '1'  # Request rate limit information in response headers
        }

        params = {
            'sid1': pv_site.pvo_sys_id,        # Target system ID from PVSite
            'd': date_.strftime('%Y%m%d'),     # Date in YYYYMMDD format
            **CONSTANT_QUERY_PARAMS
        }

        response = requests.get(URL, headers=headers, params=params)

        # Handle rate limit errors specially
        if response.status_code == 403:
            # Update rate limiter from the 403 response headers to get reset time
            self.rate_limiter.update_from_headers(response.headers)
            # Wait until rate limit resets
            self.rate_limiter.wait_if_needed()
            # Retry the request
            response = requests.get(URL, headers=headers, params=params)

        response.raise_for_status()

        # Update rate limiter from response headers
        self.rate_limiter.update_from_headers(response.headers)

        entries = _to_clean_entries(response.text)
        data = [HEADER] + entries

        return ExtractionResult(data=data)


def _to_clean_entries(text: str) -> list[list[str]]:
    lines = text.split(';')
    return [
        [_delete_if_nan(entry) for entry in line.split(',')]
        for line in lines
    ]


def _delete_if_nan(text: str) -> str:
    if text == 'NaN':
        return ''
    return text


def _remove_nans(rows: list[list[str]]) -> list[list[str]]:
    return [[field if field != '-1.#IND' else '' for field in row] for row in rows]