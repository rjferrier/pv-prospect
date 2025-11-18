"""Custom JSON serialization for Celery tasks."""

from datetime import date
from kombu.utils.json import register_type
from pv_prospect.common import DateRange
from pv_prospect.data_extraction.extractors import SourceDescriptor
from .value_objects import Task, Result, ResultType, FailureDetails


def _encode_date(obj):
    """Encode a date object to ISO format string."""
    return {'__type__': 'date', 'value': obj.isoformat()}


def _decode_date(obj):
    """Decode a date from ISO format string."""
    return date.fromisoformat(obj['value'])


def _encode_date_range(obj):
    """Encode a DateRange to a dict."""
    return {
        '__type__': 'DateRange',
        'start': obj.start.isoformat(),
        'end': obj.end.isoformat()
    }


def _decode_date_range(obj):
    """Decode a DateRange from a dict."""
    start = date.fromisoformat(obj['start'])
    end = date.fromisoformat(obj['end'])
    return DateRange(start, end)


def _encode_source_descriptor(obj):
    """Encode a SourceDescriptor enum to its value."""
    return {'__type__': 'SourceDescriptor', 'value': obj.value}


def _decode_source_descriptor(obj):
    """Decode a SourceDescriptor enum from its value."""
    return SourceDescriptor(obj['value'])


def _encode_result_type(obj):
    """Encode a ResultType enum to its value."""
    return {'__type__': 'ResultType', 'value': obj.value}


def _decode_result_type(obj):
    """Decode a ResultType enum from its value."""
    return ResultType(obj['value'])


def _encode_task(obj):
    """Encode a Task to a dict."""
    return {
        '__type__': 'Task',
        'source_descriptor': obj.source_descriptor.value if hasattr(obj.source_descriptor, 'value') else obj.source_descriptor,
        'pv_system_id': obj.pv_system_id,
        'date_range': {
            'start': obj.date_range.start.isoformat(),
            'end': obj.date_range.end.isoformat()
        }
    }


def _decode_task(obj):
    """Decode a Task from a dict."""
    source_desc = obj['source_descriptor']
    # If it's already a SourceDescriptor, use it; otherwise, convert from string
    if isinstance(source_desc, SourceDescriptor):
        source_descriptor = source_desc
    else:
        source_descriptor = SourceDescriptor(source_desc)

    date_range = DateRange(
        date.fromisoformat(obj['date_range']['start']),
        date.fromisoformat(obj['date_range']['end'])
    )
    return Task(
        source_descriptor=source_descriptor,
        pv_system_id=obj['pv_system_id'],
        date_range=date_range
    )


def _encode_failure_details(obj):
    """Encode FailureDetails to a dict."""
    return {
        '__type__': 'FailureDetails',
        'error': str(obj.error)  # Store exception as string
    }


def _decode_failure_details(obj):
    """Decode FailureDetails from a dict."""
    # Reconstruct as a generic Exception with the error message
    return FailureDetails(error=Exception(obj['error']))


def _encode_result(obj):
    """Encode a Result to a dict."""
    # Handle source_descriptor whether it's already a string or an enum
    source_desc_value = obj.task.source_descriptor.value if hasattr(obj.task.source_descriptor, 'value') else obj.task.source_descriptor

    return {
        '__type__': 'Result',
        'task': {
            'source_descriptor': source_desc_value,
            'pv_system_id': obj.task.pv_system_id,
            'date_range': {
                'start': obj.task.date_range.start.isoformat(),
                'end': obj.task.date_range.end.isoformat()
            }
        },
        'type': obj.type.value if hasattr(obj.type, 'value') else obj.type,
        'failure_details': {
            'error': str(obj.failure_details.error)
        } if obj.failure_details else None
    }


def _decode_result(obj):
    """Decode a Result from a dict."""
    task = Task(
        source_descriptor=SourceDescriptor(obj['task']['source_descriptor']),
        pv_system_id=obj['task']['pv_system_id'],
        date_range=DateRange(
            date.fromisoformat(obj['task']['date_range']['start']),
            date.fromisoformat(obj['task']['date_range']['end'])
        )
    )
    result_type = ResultType(obj['type'])
    failure_details = FailureDetails(error=Exception(obj['failure_details']['error'])) if obj['failure_details'] else None

    return Result(task=task, type=result_type, failure_details=failure_details)


def register_custom_types():
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
    
    # Register SourceDescriptor
    register_type(
        SourceDescriptor,
        'SourceDescriptor',
        _encode_source_descriptor,
        _decode_source_descriptor,
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
