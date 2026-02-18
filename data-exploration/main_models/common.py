"""
Common setup for solar power prediction notebooks.

Loads the dataset, splits into train/test sets, selects features,
one-hot encodes categorical columns, and applies StandardScaler to
continuous features. After running this script the following variables
are available in the notebook's namespace:

    data                 – raw DataFrame (all columns, all rows)
    train_set            – training split (selected features + 'power')
    test_set             – test split (selected features + 'power')
    train_features       – training features (no 'power' column), with
                           'weather_code' replaced by OHE dummy columns
    train_target         – training target (Series)
    scaler               – fitted StandardScaler (continuous features only)
    train_scaled         – scaled training features (numpy array)
    train_scaled_df      – scaled training features (DataFrame)
    WEATHER_CODE_LABELS  – dict mapping numeric weather codes → label strings
"""

from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

DATASET_PATH = Path('../../data/data-3/timeseries/61272.csv')
TARGET_COLUMN = 'power'

data = pd.read_csv(DATASET_PATH)
data['time'] = pd.to_datetime(data['time'])
data['day_of_year'] = data['time'].dt.dayofyear

# ---------------------------------------------------------------------------
# Feature selection  (comment / uncomment columns as needed)
# ---------------------------------------------------------------------------

CONTINUOUS_FEATURES = [
    'day_of_year',
    'temperature_2m',
    # 'relative_humidity_2m',
    # 'cloud_cover',
    # 'wind_speed_80m',
    # 'wind_speed_180m',
    # 'direct_normal_irradiance',
    # 'diffuse_radiation',
    # 'visibility',
    'plane_of_array_irradiance',
]

# Columns that are categorical and should be one-hot encoded rather than scaled
CATEGORICAL_FEATURES = [
    # 'weather_code'
]

# ---------------------------------------------------------------------------
# Map numeric weather codes to human-readable labels
# (applied before the train/test split so both splits share the same labels)
# ---------------------------------------------------------------------------

WEATHER_CODE_LABELS = {
    0:  'clear_sky',
    1:  'mainly_clear',
    2:  'partly_cloudy',
    3:  'overcast',
    45: 'fog',
    48: 'rime_fog',
    51: 'drizzle_light',
    53: 'drizzle_moderate',
    55: 'drizzle_dense',
    56: 'freezing_drizzle_light',
    57: 'freezing_drizzle_dense',
    61: 'rain_slight',
    63: 'rain_moderate',
    65: 'rain_heavy',
    66: 'freezing_rain_light',
    67: 'freezing_rain_heavy',
    71: 'snow_slight',
    73: 'snow_moderate',
    75: 'snow_heavy',
    77: 'snow_grains',
    80: 'rain_showers_slight',
    81: 'rain_showers_moderate',
    82: 'rain_showers_violent',
    85: 'snow_showers_slight',
    86: 'snow_showers_heavy',
    95: 'thunderstorm',
    96: 'thunderstorm_slight_hail',
    99: 'thunderstorm_heavy_hail',
}

data = data[CONTINUOUS_FEATURES + CATEGORICAL_FEATURES + [TARGET_COLUMN]]

if 'weather_code' in data.columns:
    # Apply the label mapping; unrecognised codes fall back to 'code_N'
    data['weather_code'] = data['weather_code'].map(
        lambda x: WEATHER_CODE_LABELS.get(int(x), f'code_{int(x)}')
    )

# ---------------------------------------------------------------------------
# Train / test split
# ---------------------------------------------------------------------------

train_set, test_set = train_test_split(data, test_size=0.2, random_state=42)
train_set.sort_index(inplace=True)

# ---------------------------------------------------------------------------
# Separate features and target
# ---------------------------------------------------------------------------

train_features_raw = train_set.drop(TARGET_COLUMN, axis=1)
train_target = train_set[TARGET_COLUMN]

# ---------------------------------------------------------------------------
# One-hot encode categorical features
# ---------------------------------------------------------------------------

if CATEGORICAL_FEATURES:
    # Build OHE dummies; drop_first=False keeps all categories for interpretability.
    # The dummies are cast to int so they blend naturally with the numeric columns.
    ohe_dummies = pd.get_dummies(
        train_features_raw[CATEGORICAL_FEATURES],
        columns=CATEGORICAL_FEATURES,
        drop_first=False,
        dtype=int,
    )
    train_features = pd.concat([train_features_raw[CONTINUOUS_FEATURES], ohe_dummies], axis=1)
else:
    train_features = train_features_raw[CONTINUOUS_FEATURES]

print("Features used for modelling:")
print(list(train_features.columns))
print(f"\nNumber of samples: {len(train_features)}")
print(f"Number of features: {train_features.shape[1]}")

# ---------------------------------------------------------------------------
# Feature scaling  (continuous features only)
# ---------------------------------------------------------------------------

scaler = StandardScaler()
train_continuous_scaled = scaler.fit_transform(train_features[CONTINUOUS_FEATURES])

# Re-assemble: scaled continuous columns + untouched OHE columns
train_scaled_df = pd.DataFrame(
    train_continuous_scaled,
    columns=CONTINUOUS_FEATURES,
    index=train_features.index,
)
if CATEGORICAL_FEATURES:
    train_scaled_df = pd.concat([train_scaled_df, train_features[ohe_dummies.columns]], axis=1)

train_scaled = train_scaled_df.to_numpy()

print("\n" + "="*70)
print("FEATURE SCALING APPLIED  (continuous features only)")
print("="*70)
print("\nOriginal continuous-feature statistics:")
print(train_features[CONTINUOUS_FEATURES].describe())
print("\n" + "="*70)
print("Scaled continuous-feature statistics (mean≈0, std≈1):")
print(train_scaled_df[CONTINUOUS_FEATURES].describe())
