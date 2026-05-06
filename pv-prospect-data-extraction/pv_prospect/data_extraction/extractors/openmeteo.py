import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Callable, Collection, Optional

import requests  # type: ignore[import-untyped]
from typing_extensions import deprecated

from pv_prospect.common.domain import AnySite, Location, Period
from pv_prospect.data_extraction import TimeSeries
from pv_prospect.data_extraction.util import retry_on_429

logger = logging.getLogger(__name__)

DEFAULT_SPLIT_PERIOD: Period | None = None

MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)

GRID_STEP = 0.2

CONSTANT_QUERY_PARAMS: dict[str, Any] = {
    # 'timezone': 'Europe/London',
}


def to_inclusive_datetime_bounds(
    start_date: date, end_date_exclusive: Optional[date] = None
) -> tuple[datetime, datetime]:
    """Translate an exclusive-end date range to OpenMeteo's inclusive datetime bounds.

    OpenMeteo's APIs treat their date/hour limit parameters as inclusive on both
    ends. Callers in this codebase use exclusive-end :class:`DateRange`
    semantics, so we must subtract one day from the exclusive end.

    When *end_date_exclusive* is ``None`` the request covers just *start_date*.
    """
    inclusive_end_date = (
        (end_date_exclusive - timedelta(days=1))
        if end_date_exclusive is not None
        else start_date
    )
    return (
        datetime.combine(start_date, MIN_TIME),
        datetime.combine(inclusive_end_date, MAX_TIME),
    )


@dataclass(frozen=True)
class TimeResolutionData:
    om_descriptor: str


class TimeResolution(Enum):
    HOURLY = TimeResolutionData(om_descriptor='hourly')
    QUARTERHOURLY = TimeResolutionData(om_descriptor='minutely_15')

    @property
    def om_descriptor(self) -> str:
        return self.value.om_descriptor


@dataclass(frozen=True)
class APISelectorData:
    base_url: str
    time_limit_suffix_supplier: Callable[[TimeResolution], str]
    time_stringifier: Callable[[datetime], str]
    other_parameters: Optional[dict[str, str]] = None


class APISelector(Enum):
    FORECAST = APISelectorData(
        base_url='https://api.open-meteo.com/v1/forecast',
        time_limit_suffix_supplier=lambda time_res: (
            'hour' if time_res is TimeResolution.HOURLY else time_res.om_descriptor
        ),
        time_stringifier=lambda dt: dt.isoformat(),
    )
    HISTORICAL = APISelectorData(
        base_url='https://historical-forecast-api.open-meteo.com/v1/forecast',
        time_limit_suffix_supplier=lambda time_res: 'date',
        time_stringifier=lambda dt: dt.strftime('%Y-%m-%d'),
    )

    @property
    def base_url(self) -> str:
        return self.value.base_url

    def get_time_limit_params(
        self,
        time_resolution: TimeResolution,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> dict[str, str]:
        suffix = self.value.time_limit_suffix_supplier(time_resolution)
        start = self.value.time_stringifier(start_datetime)
        end = self.value.time_stringifier(end_datetime)
        return {
            f'start_{suffix}': start,
            f'end_{suffix}': end,
        }

    @property
    def other_params(self) -> dict[str, str]:
        return self.value.other_parameters if self.value.other_parameters else {}


class Variables(Enum):
    BEST_FIVE = (
        'temperature_2m',
        'relative_humidity_2m',
        'cloud_cover',
        'direct_normal_irradiance',
        'diffuse_radiation',
    )
    BEST_TEN = (
        'temperature_2m',
        'relative_humidity_2m',
        'pressure_msl',
        'cloud_cover',
        'wind_speed_180m',
        'wind_direction_180m',
        'direct_normal_irradiance',
        'diffuse_radiation',
        'visibility',
        'weather_code',
    )

    @property
    def count(self) -> int:
        return len(self.value)

    def comma_separated(self) -> str:
        return ','.join(self.value)


class Models(Enum):
    BEST = ('best_match',)
    BEST_TWO = (
        'best_match',
        'ukmo_seamless',
    )
    ALL = (
        'best_match',
        'dmi_seamless',
        'gem_seamless',
        'gfs_seamless',
        'icon_seamless',
        'jma_seamless',
        'kma_seamless',
        'knmi_seamless',
        'meteofrance_seamless',
        'meteoswiss_icon_seamless',
        'metno_seamless',
        'ukmo_seamless',
    )

    @property
    def count(self) -> int:
        return len(self.value)

    def comma_separated(self) -> str:
        return ','.join(self.value)


@dataclass(frozen=True)
class APIHelper:
    api_selector: APISelector
    time_resolution: TimeResolution
    variables: Variables
    models: Models

    def get_url(self) -> str:
        return self.api_selector.base_url

    def get_query_params(
        self,
        locations: Collection[Location],
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> dict[str, str]:
        return {
            **self._get_location_params(locations),
            **self.api_selector.get_time_limit_params(
                self.time_resolution, start_datetime, end_datetime
            ),
            **self._get_variables_param(),
            **self._get_models_param(),
            **self.api_selector.other_params,
            **CONSTANT_QUERY_PARAMS,
        }

    @staticmethod
    def _get_location_params(locations: Collection[Location]) -> dict[str, str]:
        return {
            'latitude': ','.join(f'{loc.latitude:.4f}' for loc in locations),
            'longitude': ','.join(f'{loc.longitude:.4f}' for loc in locations),
        }

    def _get_variables_param(self) -> dict[str, str]:
        return {self.time_resolution.om_descriptor: self.variables.comma_separated()}

    def _get_models_param(self) -> dict[str, str]:
        return {'models': self.models.comma_separated()}


@deprecated('Mode enum is deprecated')
class Mode(Enum):
    QUARTERHOURLY = 'quarterhourly'
    HOURLY = 'hourly'


class OpenMeteoWeatherDataExtractor:
    def __init__(self, api_helper: APIHelper) -> None:
        self.api_helper = api_helper

    @classmethod
    def from_components(
        cls,
        api_selector: APISelector,
        time_resolution: TimeResolution,
        models: Models,
        variables: Variables,
        max_model_variables: int,
    ) -> 'OpenMeteoWeatherDataExtractor':
        n = models.count * variables.count
        if n > max_model_variables:
            raise ValueError(
                f'{models.name} ({models.count} models) × {variables.name} '
                f'({variables.count} variables) = {n} model-variables, '
                f'exceeds the configured limit of {max_model_variables}.'
            )
        return cls(
            api_helper=APIHelper(
                api_selector=api_selector,
                time_resolution=time_resolution,
                models=models,
                variables=variables,
            ),
        )

    @retry_on_429
    def extract(
        self,
        sites: Collection[AnySite],
        date_: date,
        end_date: Optional[date] = None,
    ) -> list[TimeSeries]:
        """Extract time series data for *sites* over ``[date_, end_date)``.

        ``end_date`` is exclusive (matching :class:`DateRange` semantics).
        When ``end_date`` is ``None`` the request covers only *date_*.
        """
        sites_list = list(sites)

        start_datetime, end_datetime = to_inclusive_datetime_bounds(date_, end_date)

        locations = [s.location for s in sites_list]

        url = self.api_helper.get_url()
        params = self.api_helper.get_query_params(
            locations, start_datetime, end_datetime
        )
        response = requests.get(url=url, params=params, timeout=60)
        if response.status_code >= 400:
            logger.error(
                'API error: status=%d url=%s text=%s',
                response.status_code,
                response.url,
                response.text,
            )
        response.raise_for_status()

        # Parse JSON response and convert to CSV rows
        json_data = json.loads(response.text)

        if len(sites_list) == 1:
            return [
                TimeSeries(
                    rows=self._process_time_series_data(json_data),
                    metadata=_metadata_from_response(json_data),
                )
            ]
        else:
            return [
                TimeSeries(
                    rows=self._process_time_series_data(entry),
                    metadata=_metadata_from_response(entry),
                )
                for entry in json_data
            ]

    def _process_time_series_data(self, json_data: dict[str, Any]) -> list[list[str]]:
        om_descriptor = self.api_helper.time_resolution.om_descriptor

        try:
            time_series_data = json_data[om_descriptor]
        except KeyError as e:
            raise ValueError(
                f"Expected time resolution '{om_descriptor}' not found in response:\n"
                f'{json.dumps(json_data)}'
            ) from e

        # Get the headers (field names) and corresponding arrays
        headers = list(time_series_data.keys())
        arrays = [time_series_data[header] for header in headers]

        # Ensure all arrays have the same length
        if not arrays or not arrays[0]:
            return []

        num_rows = len(arrays[0])

        # Build CSV rows: header row + data rows
        rows = [headers]
        for i in range(num_rows):
            row = [str(array[i]) if array[i] is not None else '' for array in arrays]
            rows.append(row)

        return rows


def _metadata_from_response(json_data: dict[str, Any]) -> dict[str, Any]:
    """Extract location metadata from the API response."""
    return {
        'latitude': json_data['latitude'],
        'longitude': json_data['longitude'],
        'elevation': json_data['elevation'],
    }
