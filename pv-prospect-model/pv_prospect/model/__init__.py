"""PV Prospect ML model training package."""

from pv_prospect.model.evaluation import clamped_power_pred, eval_in_f_space
from pv_prospect.model.inference import predict_capacity_factor, predict_weather
from pv_prospect.model.persistence import load_artifact, load_weather_artifact

__version__ = '0.1.0'

__all__ = [
    'clamped_power_pred',
    'eval_in_f_space',
    'load_artifact',
    'load_weather_artifact',
    'predict_capacity_factor',
    'predict_weather',
]
