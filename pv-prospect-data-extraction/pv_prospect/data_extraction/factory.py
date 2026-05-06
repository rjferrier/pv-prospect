from functools import lru_cache

from pv_prospect.common import get_config
from pv_prospect.common.domain import Period
from pv_prospect.data_extraction import TimeSeriesDataExtractor
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.extractors import (
    OpenMeteoWeatherDataExtractor,
    PVOutputExtractor,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    DEFAULT_SPLIT_PERIOD as _OPENMETEO_DEFAULT_SPLIT_PERIOD,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    APISelector,
    Models,
    TimeResolution,
    Variables,
)
from pv_prospect.data_extraction.extractors.pvoutput import (
    DEFAULT_SPLIT_PERIOD as _PVOUTPUT_DEFAULT_SPLIT_PERIOD,
)
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import DataSource
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import get_config_dir as get_etl_config_dir


@lru_cache(maxsize=None)
def get_extractor(data_source: DataSource) -> TimeSeriesDataExtractor:
    config = get_config(
        DataExtractionConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_de_config_dir(),
        ],
    )
    return _make_extractor(data_source, config.openmeteo.max_model_variables)


def _make_extractor(
    data_source: DataSource, max_model_variables: int
) -> TimeSeriesDataExtractor:
    if data_source == DataSource.PVOUTPUT:
        return PVOutputExtractor.from_env()
    if data_source == DataSource.OPENMETEO_QUARTERHOURLY:
        return OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.QUARTERHOURLY,
            variables=Variables.BEST_FIVE,
            models=Models.BEST_TWO,
            max_model_variables=max_model_variables,
        )
    if data_source == DataSource.OPENMETEO_HOURLY:
        return OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.HOURLY,
            variables=Variables.BEST_FIVE,
            models=Models.BEST_TWO,
            max_model_variables=max_model_variables,
        )
    if data_source == DataSource.OPENMETEO_HISTORICAL:
        return OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.HISTORICAL,
            time_resolution=TimeResolution.HOURLY,
            variables=Variables.BEST_FIVE,
            models=Models.BEST_TWO,
            max_model_variables=max_model_variables,
        )
    raise ValueError(f'Unsupported data source: {data_source}')


_MULTI_DATE_EXTRACTORS = {
    DataSource.OPENMETEO_HISTORICAL,
}


def supports_multi_date(data_source: DataSource) -> bool:
    return data_source in _MULTI_DATE_EXTRACTORS


_DEFAULT_SPLIT_PERIODS: dict[DataSource, Period | None] = {
    DataSource.PVOUTPUT: _PVOUTPUT_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_QUARTERHOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_HOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_HISTORICAL: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
}


def default_split_period(data_source: DataSource) -> Period | None:
    return _DEFAULT_SPLIT_PERIODS[data_source]
