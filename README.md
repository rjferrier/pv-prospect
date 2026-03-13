# pv-prospect

A model of photovoltaic (PV) power outputs according to weather in the UK. This may be
useful if you are thinking about installing solar panels and would like to know how much
energy you could get from them.

This is a work in progress.

## About the Model

The model is a pair of deep neural networks trained on the power output data of 10 existing
PV systems and corresponding weather data. The PV data has been kindly uploaded by the
system owners to [PVOutput](https://pvoutput.org/). Historical weather data is available
from [Open-Meteo](https://open-meteo.com/).

To predict energy yield, the model requires the following inputs:

* location (latitude and longitude)
* period (start and end dates)
* solar panel power rating
* azimuthal orientation (angle clockwise from North)
* tilt (angle from the horizontal)

From the location and period, the first neural network will predict a time series of
relevant weather variables such as temperature, direct normal irradiance (DNI) and
diffuse horizontal irradiance (DHI). From irradiance data and the solar panel features,
plane-of-array (POA) irradiance can be calculated. The second neural network will use
POA irradiance and other features to estimate power output over time.

Some caveats:

* The model does not factor in shade from trees and other obstacles. These
  could negatively impact the actual power output.
* The model is trained on systems that may have old or ageing technology.
  Newer technology could mean the actual power output is higher than expected.
