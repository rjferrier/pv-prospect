"""Shared test data for pv_site_repo tests."""

CSV_HEADER = (
    'pvoutput_system_id,name,latitude,longitude,shading,'
    'panel_brand,panels_capacity,inverter_brand,inverter_capacity,'
    'panel_azimuth_1,panel_elevation_1,panel_area_fraction_1,'
    'panel_azimuth_2,panel_elevation_2,panel_area_fraction_2,'
    'installation_date'
)

SAMPLE_ROW_STR = (
    '89665,Test Site,51.5074,-0.1278,NONE,'
    'SunPower,4000,Fronius,3600,'
    '180,30,1.0,'
    ',,,'
    '2020-06-15'
)

TWO_PANEL_ROW_STR = (
    '12345,Dual Panel,52.0,1.0,LOW,LG,5000,SMA,4500,160,25,0.6,200,35,0.4,2021-01-01'
)


def make_csv_row() -> dict[str, str]:
    keys = CSV_HEADER.split(',')
    values = SAMPLE_ROW_STR.split(',')
    return dict(zip(keys, values, strict=True))


def make_two_panel_row() -> dict[str, str]:
    keys = CSV_HEADER.split(',')
    values = TWO_PANEL_ROW_STR.split(',')
    return dict(zip(keys, values, strict=True))
