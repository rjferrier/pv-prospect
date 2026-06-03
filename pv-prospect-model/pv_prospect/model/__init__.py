"""PV Prospect ML model training package."""

from pv_prospect.model.inference import predict_capacity_factor, predict_weather

__version__ = '0.1.0'

__all__ = ['predict_capacity_factor', 'predict_weather']
