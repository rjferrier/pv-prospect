from datetime import date, datetime
from typing import Optional
import time

import requests

from domain import PVSite
from extractors import ExtractionResult
from util import map_from_env, VarMapping


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
    """

    def __init__(self):
        self.rate_limit: Optional[int] = None
        self.remaining_requests: Optional[int] = None
        self.reset_time: Optional[datetime] = None
        self._first_update = True

    def update_from_headers(self, headers: dict):
        """
        Update rate limit info from API response headers.

        Args:
            headers: Response headers from PVOutput API
        """
        if RATE_LIMIT_HEADER in headers:
            self.rate_limit = int(headers[RATE_LIMIT_HEADER])

        if RATE_LIMIT_REMAINING_HEADER in headers:
            self.remaining_requests = int(headers[RATE_LIMIT_REMAINING_HEADER])

        if RATE_LIMIT_RESET_HEADER in headers:
            # The reset time is in Unix timestamp (seconds since epoch)
            reset_timestamp = int(headers[RATE_LIMIT_RESET_HEADER])
            self.reset_time = datetime.fromtimestamp(reset_timestamp)

        # Print rate limit info on first update
        if self._first_update and self.rate_limit is not None:
            reset_time_str = self.reset_time.strftime('%H:%M:%S') if self.reset_time else 'unknown'
            print(f"ℹ️  Rate limit info: {self.remaining_requests}/{self.rate_limit} requests remaining (resets at {reset_time_str})")
            self._first_update = False

    def wait_if_needed(self):
        """
        Check if we need to wait for rate limit to reset.
        If remaining requests is 0 or very low, sleep until reset time.
        """
        if self.remaining_requests is not None and self.remaining_requests <= 0:
            if self.reset_time:
                current_time = datetime.now()
                if current_time < self.reset_time:
                    sleep_seconds = (self.reset_time - current_time).total_seconds()
                    if sleep_seconds > 0:
                        limit_info = f" (limit: {self.rate_limit})" if self.rate_limit else ""
                        print(f"\n⚠️  Rate limit reached{limit_info} - 0 requests remaining.")
                        print(f"   Sleeping for {sleep_seconds:.1f} seconds until {self.reset_time.strftime('%H:%M:%S')}...\n")
                        time.sleep(sleep_seconds)
                        # Reset after sleeping
                        self.remaining_requests = None
                        self.reset_time = None


class PVOutputExtractor:
    multi_date = False  # This extractor processes one date at a time

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

        # Check rate limit before making the request
        self.rate_limiter.wait_if_needed()

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