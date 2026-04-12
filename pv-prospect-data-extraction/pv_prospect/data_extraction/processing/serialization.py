"""Custom JSON serialization for Celery tasks."""

from datetime import date
from decimal import Decimal

from kombu.utils.json import register_type

from pv_prospect.common.domain import (
    AnySite,
    ArbitrarySite,
    DateRange,
    Location,
    PVSite,
)
from pv_prospect.data_extraction import DataSource
from pv_prospect.data_extraction.processing.value_objects import (
    FailureDetails,
    Result,
    ResultType,
    Task,
)


def _encode_date(obj: date) -> dict[str, str]:
    """Encode a date object to ISO format string."""
    return {'__type__': 'date', 'value': obj.isoformat()}


def _decode_date(obj: dict[str, str]) -> date:
    """Decode a date from ISO format string."""
    return date.fromisoformat(obj['value'])


def _encode_date_range(obj: DateRange) -> dict[str, str]:
    """Encode a DateRange to a dict."""
    return {
        '__type__': 'DateRange',
        'start': obj.start.isoformat(),
        'end': obj.end.isoformat(),
    }


def _decode_date_range(obj: dict[str, str]) -> DateRange:
    """Decode a DateRange from a dict."""
    start = date.fromisoformat(obj['start'])
    end = date.fromisoformat(obj['end'])
    return DateRange(start, end)


def _encode_data_source(obj: DataSource) -> dict[str, str]:
    """Encode a DataSource enum to its value."""
    return {'__type__': 'DataSource', 'value': obj.value}


def _decode_data_source(obj: dict[str, str]) -> DataSource:
    """Decode a DataSource enum from its value."""
    return DataSource(obj['value'])


def _encode_result_type(obj: ResultType) -> dict[str, str]:
    """Encode a ResultType enum to its value."""
    return {'__type__': 'ResultType', 'value': obj.value}


def _decode_result_type(obj: dict[str, str]) -> ResultType:
    """Decode a ResultType enum from its value."""
    return ResultType(obj['value'])


def _encode_target(site: AnySite) -> dict:
    """Encode a PVSite or ArbitrarySite to a dict."""
    if isinstance(site, PVSite):
        return {
            '__target_type__': 'pv_site',
            'pvo_sys_id': site.pvo_sys_id,
        }
    return {
        '__target_type__': 'arbitrary_site',
        'latitude': str(site.location.latitude),
        'longitude': str(site.location.longitude),
    }


def _decode_target(obj: dict) -> AnySite:
    """Decode a PVSite or ArbitrarySite from a dict."""
    if obj['__target_type__'] in ('arbitrary_site', 'grid_point', 'location'):
        return ArbitrarySite(
            Location(
                latitude=Decimal(obj['latitude']),
                longitude=Decimal(obj['longitude']),
            )
        )
    # PVSite: reconstruct from in-memory repo
    from pv_prospect.common import get_pv_site_by_system_id

    return get_pv_site_by_system_id(obj['pvo_sys_id'])


def _encode_task(obj: Task) -> dict:
    """Encode a Task to a dict."""
    return {
        '__type__': 'Task',
        'data_source': obj.data_source.value
        if hasattr(obj.data_source, 'value')
        else obj.data_source,
        'target': _encode_target(obj.site),
        'date_range': {
            'start': obj.date_range.start.isoformat(),
            'end': obj.date_range.end.isoformat(),
        },
    }


def _decode_task(obj: dict) -> Task:
    """Decode a Task from a dict."""
    source_desc = obj['data_source']
    if isinstance(source_desc, DataSource):
        data_source = source_desc
    else:
        data_source = DataSource(source_desc)

    date_range = DateRange(
        date.fromisoformat(obj['date_range']['start']),
        date.fromisoformat(obj['date_range']['end']),
    )
    return Task(
        data_source=data_source,
        site=_decode_target(obj['target']),
        date_range=date_range,
    )


def _encode_failure_details(obj: FailureDetails) -> dict[str, str]:
    """Encode FailureDetails to a dict."""
    return {
        '__type__': 'FailureDetails',
        'error': str(obj.error),  # Store exception as string
    }


def _decode_failure_details(obj: dict[str, str]) -> FailureDetails:
    """Decode FailureDetails from a dict."""
    # Reconstruct as a generic Exception with the error message
    return FailureDetails(error=Exception(obj['error']))


def _encode_result(obj: Result) -> dict:
    """Encode a Result to a dict."""
    source_desc_value = (
        obj.task.data_source.value
        if hasattr(obj.task.data_source, 'value')
        else obj.task.data_source
    )

    return {
        '__type__': 'Result',
        'task': {
            'data_source': source_desc_value,
            'target': _encode_target(obj.task.site),
            'date_range': {
                'start': obj.task.date_range.start.isoformat(),
                'end': obj.task.date_range.end.isoformat(),
            },
        },
        'type': obj.type.value if hasattr(obj.type, 'value') else obj.type,
        'failure_details': {'error': str(obj.failure_details.error)}
        if obj.failure_details
        else None,
    }


def _decode_result(obj: dict) -> Result:
    """Decode a Result from a dict."""
    task = Task(
        data_source=DataSource(obj['task']['data_source']),
        site=_decode_target(obj['task']['target']),
        date_range=DateRange(
            date.fromisoformat(obj['task']['date_range']['start']),
            date.fromisoformat(obj['task']['date_range']['end']),
        ),
    )
    result_type = ResultType(obj['type'])
    failure_details = (
        FailureDetails(error=Exception(obj['failure_details']['error']))
        if obj['failure_details']
        else None
    )

    return Result(task=task, type=result_type, failure_details=failure_details)


def register_custom_types() -> None:
    """Register custom types with Kombu's JSON serializer."""
    # Register date
    register_type(
        date,
        'date',
        _encode_date,
        _decode_date,
    )

    # Register DateRange
    register_type(
        DateRange,
        'DateRange',
        _encode_date_range,
        _decode_date_range,
    )

    # Register DataSource
    register_type(
        DataSource,
        'DataSource',
        _encode_data_source,
        _decode_data_source,
    )

    # Register ResultType
    register_type(
        ResultType,
        'ResultType',
        _encode_result_type,
        _decode_result_type,
    )

    # Register Task
    register_type(
        Task,
        'Task',
        _encode_task,
        _decode_task,
    )

    # Register FailureDetails
    register_type(
        FailureDetails,
        'FailureDetails',
        _encode_failure_details,
        _decode_failure_details,
    )

    # Register Result
    register_type(
        Result,
        'Result',
        _encode_result,
        _decode_result,
    )
