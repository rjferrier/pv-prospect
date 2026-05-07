from enum import Enum


class Transformation(Enum):
    CLEAN_WEATHER = 'clean_weather'
    CLEAN_PV = 'clean_pv'
    PREPARE_WEATHER = 'prepare_weather'
    PREPARE_PV = 'prepare_pv'
    ASSEMBLE_WEATHER = 'assemble_weather'
    ASSEMBLE_PV = 'assemble_pv'
    CONSOLIDATE_LOGS = 'consolidate_logs'


ALL_TRANSFORMATIONS = (
    Transformation.CLEAN_WEATHER,
    Transformation.CLEAN_PV,
    Transformation.PREPARE_WEATHER,
    Transformation.PREPARE_PV,
    Transformation.ASSEMBLE_WEATHER,
    Transformation.ASSEMBLE_PV,
)

CLEANING_TRANSFORMATIONS: frozenset[Transformation] = frozenset(
    {
        Transformation.CLEAN_WEATHER,
        Transformation.CLEAN_PV,
    }
)

PREPARING_TRANSFORMATIONS: frozenset[Transformation] = frozenset(
    {
        Transformation.PREPARE_WEATHER,
        Transformation.PREPARE_PV,
    }
)

TRANSFORMATIONS_NEEDING_PV_SITE: frozenset[Transformation] = frozenset(
    {Transformation.CLEAN_PV, Transformation.PREPARE_PV, Transformation.ASSEMBLE_PV}
)

TRANSFORMATIONS_NEEDING_GRID_POINT: frozenset[Transformation] = frozenset(
    {
        Transformation.CLEAN_WEATHER,
        Transformation.PREPARE_WEATHER,
    }
)
