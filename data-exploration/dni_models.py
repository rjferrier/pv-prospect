"""
DNI (Direct Normal Irradiance) calculation models and utilities.

This module provides various methods for calculating DNI including:
- Solarpy-based calculations
- pvlib clear-sky models (Ineichen, Simplified Solis)
- DQYDJ reference data loading

Author: Data Exploration Team
Date: December 2025
"""
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pvlib
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Optional
from solarpy import standard2solar_time, beam_irradiance

# This should be between 0.17 (optimum for Simplified Solis) and 0.33 (for Ineichen)
MIN_DNI_COEFFICIENT = 0.2


class DniModel(Enum):
    """Enum representing different DNI calculation models."""
    SOLARPY = 'solarpy'
    PVLIB_INEICHEN = 'pvlib_ineichen'
    PVLIB_SIMPLIFIED_SOLIS = 'pvlib_simplified_solis'
    DQYDJ = 'dqydj'

    def __str__(self):
        return self.display_name

    @property
    def display_name(self) -> str:
        """Get display name for the model."""
        if self == DniModel.SOLARPY:
            return 'Solarpy'
        elif self == DniModel.PVLIB_INEICHEN:
            return 'pvlib-Ineichen'
        elif self == DniModel.PVLIB_SIMPLIFIED_SOLIS:
            return 'pvlib-SimplifiedSolis'
        elif self == DniModel.DQYDJ:
            return 'DQYDJ'
        return self.value

    @property
    def column_name(self) -> Optional[str]:
        """Get the data column name for the model."""
        if self == DniModel.SOLARPY:
            return 'dni_calculated'
        elif self == DniModel.PVLIB_INEICHEN:
            return 'dni_pvlib_ineichen'
        elif self == DniModel.PVLIB_SIMPLIFIED_SOLIS:
            return 'dni_pvlib_simplified_solis'
        elif self == DniModel.DQYDJ:
            return 'dni_dqydj'
        return None

    @property
    def pvlib_model_name(self) -> Optional[str]:
        """Get the pvlib model name for get_clearsky() if this is a pvlib model."""
        if self == DniModel.PVLIB_INEICHEN:
            return 'ineichen'
        elif self == DniModel.PVLIB_SIMPLIFIED_SOLIS:
            return 'simplified_solis'
        return None


def get_cloud_cover_dni_coefficient(cloud_cover_percent):
    return np.interp(
        cloud_cover_percent / 100,
        np.array([0.0, 1.0]),
        np.array([1.0, MIN_DNI_COEFFICIENT]),
    )


def calculate_direct_normal_irradiance(
    dt: datetime,
    latitude: float,
    longitude: float,
    altitude: float
) -> float:
    """
    Calculate Direct Normal Irradiance using solarpy library.

    Args:
        dt: datetime object
        latitude: Site latitude in degrees
        longitude: Site longitude in degrees
        altitude: Site altitude in meters

    Returns:
        DNI in W/m²
    """
    solar_time = standard2solar_time(dt, longitude)
    return max(0., beam_irradiance(altitude, solar_time, latitude))


def calculate_pvlib_dni_bulk(
    times: pd.DatetimeIndex,
    latitude: float,
    longitude: float,
    altitude: float,
    model: str = 'ineichen'
) -> pd.Series:
    """
    Calculate Direct Normal Irradiance using pvlib's clear sky models.

    Note: pvlib works with collections of timestamps, not individual timestamps.
    Note: Only 'ineichen' and 'simplified_solis' models provide DNI.
          The 'haurwitz' model only provides GHI and cannot be used for DNI.

    Args:
        times: DatetimeIndex of timestamps (will be localized to UTC if needed)
        latitude: Site latitude in degrees
        longitude: Site longitude in degrees
        altitude: Site altitude in meters
        model: Clear sky model to use. Options: 'ineichen', 'simplified_solis'

    Returns:
        Series of DNI values in W/m²
    """
    # Create location object
    location = pvlib.location.Location(latitude, longitude, altitude=altitude, tz='UTC')

    # Ensure times are timezone-aware
    if times.tz is None:
        times = times.tz_localize('UTC')

    # Get clear sky irradiance using specified model
    clearsky = location.get_clearsky(times, model=model)

    return clearsky['dni']


def load_dqydj_data(
    dqydj_dir: Path = Path('resources/dqydj'),
    verbose: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Load DQYDJ reference data for comparison.

    The DQYDJ data provides reference DNI measurements for specific dates
    corresponding to equinox and solstice months.

    Args:
        dqydj_dir: Path to directory containing DQYDJ CSV files
        verbose: If True, print loading status messages

    Returns:
        Dictionary mapping month names to DataFrames with columns:
        - time: datetime
        - dni_wm2: DNI in W/m²
        (and other columns from the CSV files)
    """
    dqydj_files = {
        'March': 'dqydj-solar-irradiance-20250302.csv',
        'June': 'dqydj-solar-irradiance-20230608.csv',
        'September': 'dqydj-solar-irradiance-20230905.csv',
        'December': 'dqydj-solar-irradiance-20221215.csv'
    }

    dqydj_data = {}
    for month, filename in dqydj_files.items():
        filepath = dqydj_dir / filename
        if filepath.exists():
            # Read CSV, skipping the header lines (first 6 lines are metadata)
            df = pd.read_csv(filepath, skiprows=6)
            df['time'] = pd.to_datetime(df['time'])
            dqydj_data[month] = df
            if verbose:
                print(f"Loaded DQYDJ data for {month}: {len(df)} records")
        else:
            if verbose:
                print(f"Warning: DQYDJ file not found for {month}: {filepath}")

    if verbose:
        print(f"\nLoaded DQYDJ data for {len(dqydj_data)} months")

    return dqydj_data


def calculate_dni_for_dataframe(
    df: pd.DataFrame,
    latitude: float,
    longitude: float,
    altitude: float,
    time_column: str = 'time',
    models: list = None
) -> pd.DataFrame:
    """
    Calculate DNI using multiple models for all timestamps in a DataFrame.

    Args:
        df: DataFrame containing a time column
        latitude: Site latitude in degrees
        longitude: Site longitude in degrees
        altitude: Site altitude in meters
        time_column: Name of the column containing datetime values
        models: List of DniModel enum values to calculate. If None, calculates all models
                except DQYDJ (which requires separate data files)

    Returns:
        DataFrame with additional columns for each calculated DNI model
    """
    if models is None:
        # Default to all calculation models (exclude DQYDJ which requires separate data)
        models = [DniModel.SOLARPY, DniModel.PVLIB_INEICHEN, DniModel.PVLIB_SIMPLIFIED_SOLIS]

    result_df = df.copy()

    for model in models:
        if model == DniModel.SOLARPY:
            result_df[model.column_name] = result_df[time_column].apply(
                lambda dt: calculate_direct_normal_irradiance(dt, latitude, longitude, altitude)
            )
        elif model in [DniModel.PVLIB_INEICHEN, DniModel.PVLIB_SIMPLIFIED_SOLIS]:
            times_utc = pd.DatetimeIndex(result_df[time_column])
            if times_utc.tz is None:
                times_utc = times_utc.tz_localize('UTC')
            result_df[model.column_name] = calculate_pvlib_dni_bulk(
                times_utc, latitude, longitude, altitude, model=model.pvlib_model_name
            ).values
        elif model == DniModel.DQYDJ:
            # DQYDJ requires separate data loading and cannot be calculated
            raise ValueError(
                "DQYDJ model requires loading separate reference data files. "
                "Use load_dqydj_data() instead."
            )

    return result_df
