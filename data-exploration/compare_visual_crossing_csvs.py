import pandas as pd

# Load both files
vc_hourly = pd.read_csv('../datasets/data-1/visualcrossing-hourly_89665.csv', parse_dates=['datetime'])
vc_quarterly = pd.read_csv('../datasets/data-1/visualcrossing-quarterhourly_89665.csv', parse_dates=['datetime'])

# Filter quarterly data to only hourly timestamps (on the hour)
vc_quarterly_hourly = vc_quarterly[vc_quarterly['datetime'].dt.minute == 0]

# Merge on datetime to compare
comparison = pd.merge(vc_hourly, vc_quarterly_hourly, on='datetime', suffixes=('_hourly', '_quarterly'))

# Check if values match for each column and report only mismatches
def report_mismatches(df, columns_to_compare):
    mismatches_found = False
    for col in columns_to_compare:
        hourly_col = f'{col}_hourly'
        quarterly_col = f'{col}_quarterly'
        if hourly_col in df.columns and quarterly_col in df.columns:
            mismatches = df[df[hourly_col] != df[quarterly_col]][['datetime', hourly_col, quarterly_col]]
            if not mismatches.empty:
                mismatches_found = True
                print(f'Column: {col} - {len(mismatches)} mismatches:')
                print(mismatches.to_string(index=False))
    if not mismatches_found:
        print('All compared values match for the selected columns.')

columns_to_compare = ['temp', 'humidity', 'precip', 'windspeed', 'winddir',
                      'sealevelpressure', 'cloudcover', 'visibility', 'solarradiation']

report_mismatches(comparison, columns_to_compare)
