"""Module-style unit tests for processing.serialization."""

from datetime import date

from processing.serialization import (
    _encode_date, _decode_date,
    _encode_date_range, _decode_date_range,
    _encode_source_descriptor, _decode_source_descriptor,
    _encode_result_type, _decode_result_type,
    _encode_task, _decode_task,
    _encode_failure_details, _decode_failure_details,
    _encode_result, _decode_result,
)
from domain.date_range import DateRange
from extractors.data_sources import SourceDescriptor
from processing.value_objects import Task, Result, ResultType, FailureDetails


def test_encode_date():
    d = date(2024, 1, 15)
    encoded = _encode_date(d)

    assert encoded == {'__type__': 'date', 'value': '2024-01-15'}


def test_decode_date():
    encoded = {'__type__': 'date', 'value': '2024-01-15'}
    decoded = _decode_date(encoded)

    assert decoded == date(2024, 1, 15)


def test_date_roundtrip():
    original = date(2024, 12, 31)
    encoded = _encode_date(original)
    decoded = _decode_date(encoded)

    assert decoded == original


def test_encode_date_range_with_end():
    dr = DateRange(start=date(2024, 1, 15), end=date(2024, 1, 20))
    encoded = _encode_date_range(dr)

    assert encoded == {
        '__type__': 'DateRange',
        'start': '2024-01-15',
        'end': '2024-01-20'
    }


def test_encode_date_range_single_day():
    dr = DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    encoded = _encode_date_range(dr)

    assert encoded == {
        '__type__': 'DateRange',
        'start': '2024-01-15',
        'end': '2024-01-16'
    }


def test_decode_date_range_with_end():
    encoded = {
        '__type__': 'DateRange',
        'start': '2024-01-15',
        'end': '2024-01-20'
    }
    decoded = _decode_date_range(encoded)

    assert decoded.start == date(2024, 1, 15)
    assert decoded.end == date(2024, 1, 20)


def test_decode_date_range_single_day():
    encoded = {
        '__type__': 'DateRange',
        'start': '2024-01-15',
        'end': '2024-01-16'
    }
    decoded = _decode_date_range(encoded)

    assert decoded.start == date(2024, 1, 15)
    assert decoded.end == date(2024, 1, 16)


def test_encode_source_descriptor():
    sd = SourceDescriptor.OPENMETEO_HOURLY
    encoded = _encode_source_descriptor(sd)

    assert encoded == {
        '__type__': 'SourceDescriptor',
        'value': 'openmeteo/hourly'
    }


def test_decode_source_descriptor():
    encoded = {
        '__type__': 'SourceDescriptor',
        'value': 'pvoutput'
    }
    decoded = _decode_source_descriptor(encoded)

    assert decoded == SourceDescriptor.PVOUTPUT


def test_source_descriptor_roundtrip():
    original = SourceDescriptor.VISUALCROSSING_QUARTERHOURLY
    encoded = _encode_source_descriptor(original)
    decoded = _decode_source_descriptor(encoded)

    assert decoded == original


def test_encode_result_type():
    rt = ResultType.SUCCESS
    encoded = _encode_result_type(rt)

    assert encoded == {
        '__type__': 'ResultType',
        'value': 'success'
    }


def test_decode_result_type():
    encoded = {
        '__type__': 'ResultType',
        'value': 'failure'
    }
    decoded = _decode_result_type(encoded)

    assert decoded == ResultType.FAILURE


def test_encode_task():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 20))
    )
    encoded = _encode_task(task)

    assert encoded['__type__'] == 'Task'
    assert encoded['source_descriptor'] == 'pvoutput'
    assert encoded['pv_system_id'] == 12345
    assert encoded['date_range']['start'] == '2024-01-15'
    assert encoded['date_range']['end'] == '2024-01-20'


def test_decode_task():
    encoded = {
        '__type__': 'Task',
        'source_descriptor': 'openmeteo/hourly',
        'pv_system_id': 67890,
        'date_range': {
            'start': '2024-01-15',
            'end': '2024-01-20'
        }
    }
    decoded = _decode_task(encoded)

    assert decoded.source_descriptor == SourceDescriptor.OPENMETEO_HOURLY
    assert decoded.pv_system_id == 67890
    assert decoded.date_range.start == date(2024, 1, 15)
    assert decoded.date_range.end == date(2024, 1, 20)


def test_task_roundtrip():
    original = Task(
        source_descriptor=SourceDescriptor.OPENMETEO_SATELLITE,
        pv_system_id=11111,
        date_range=DateRange(start=date(2024, 2, 1), end=date(2024, 2, 2))
    )
    encoded = _encode_task(original)
    decoded = _decode_task(encoded)

    assert decoded.source_descriptor == original.source_descriptor
    assert decoded.pv_system_id == original.pv_system_id
    assert decoded.date_range.start == original.date_range.start
    assert decoded.date_range.end == original.date_range.end


def test_encode_failure_details():
    details = FailureDetails(error=ValueError("Test error"))
    encoded = _encode_failure_details(details)

    assert encoded == {
        '__type__': 'FailureDetails',
        'error': 'Test error'
    }


def test_decode_failure_details():
    encoded = {
        '__type__': 'FailureDetails',
        'error': 'Something went wrong'
    }
    decoded = _decode_failure_details(encoded)

    assert isinstance(decoded, FailureDetails)
    assert str(decoded.error) == 'Something went wrong'


def test_encode_success_result():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )
    result = Result.success(task)
    encoded = _encode_result(result)

    assert encoded['__type__'] == 'Result'
    assert encoded['type'] == 'success'
    assert encoded['failure_details'] is None
    assert encoded['task']['pv_system_id'] == 12345


def test_encode_failure_result():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )
    result = Result.failure(task, ValueError("Test error"))
    encoded = _encode_result(result)

    assert encoded['__type__'] == 'Result'
    assert encoded['type'] == 'failure'
    assert encoded['failure_details']['error'] == 'Test error'


def test_decode_success_result():
    encoded = {
        '__type__': 'Result',
        'task': {
            'source_descriptor': 'pvoutput',
            'pv_system_id': 12345,
            'date_range': {
                'start': '2024-01-15',
                'end': '2024-01-16'
            }
        },
        'type': 'success',
        'failure_details': None
    }
    decoded = _decode_result(encoded)

    assert decoded.type == ResultType.SUCCESS
    assert decoded.task.pv_system_id == 12345
    assert decoded.failure_details is None


def test_decode_failure_result():
    encoded = {
        '__type__': 'Result',
        'task': {
            'source_descriptor': 'pvoutput',
            'pv_system_id': 12345,
            'date_range': {
                'start': '2024-01-15',
                'end': '2024-01-16'
            }
        },
        'type': 'failure',
        'failure_details': {
            'error': 'Something failed'
        }
    }
    decoded = _decode_result(encoded)

    assert decoded.type == ResultType.FAILURE
    assert decoded.failure_details is not None
    assert str(decoded.failure_details.error) == 'Something failed'


def test_result_roundtrip_success():
    task = Task(
        source_descriptor=SourceDescriptor.OPENMETEO_HOURLY,
        pv_system_id=67890,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 20))
    )
    original = Result.success(task)
    encoded = _encode_result(original)
    decoded = _decode_result(encoded)

    assert decoded.type == original.type
    assert decoded.task.source_descriptor == original.task.source_descriptor
    assert decoded.task.pv_system_id == original.task.pv_system_id
    assert decoded.failure_details is None

