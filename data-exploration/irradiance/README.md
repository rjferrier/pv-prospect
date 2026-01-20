## Derivation of Plane-Of-Array (POA) Irradiance

Open-Meteo exposes two types of irradiance: Direct Normal Irradiance (DNI) and Diffuse Horizontal Irradiance (DHI).
The question is, for each solar PV site, how much total irradiance are the panels receiving over time?
The answer is that we need to calculate plane-of-array (POA) irradiance, and there are several resources 
on the internet for helping with this. Perhaps the most useful is PVLib and
[this tutorial](https://pv-tutorials.github.io/PVPMC_2022/Tutorial%202%20-%20POA%20Irradiance.html) in particular.

The inputs to calculate POA are DNI, DHI and Global Horizontal Irradiance (GHI). We don't have GHI data so we 
need to synthesise it somehow. There are various models to help with this, but all the models assume perfect 
conditions (i.e. clear sky) whereas we want to include the effects of cloud cover as the DNI data does.

So we'll use models to create a scale factor, `f_dni_to_ghi = GHI_modelled / DNI_modelled`, for the DNI data. 
The GHI can then be calculated as

```
GHI_actual = f_dni_to_ghi * DNI_actual
```

To lend confidence in this approach, we need to make sure the modelled DNI predicts the actual DNI with high
accuracy during good weather conditions. Two notebooks were created for this purpose:

* `sunniest-days-analysis.ipynb` identifies the days with the best conditions in each of the four seasons, 
  for a given location, out of the corpus of weather data.
* `dni-comparison` compares different DNI models with DNI data from different weather models and
  identifies the DNI model and weather model that showed the best agreement. A cloud cover correction
  was introduced to help compare data in non-ideal conditions, but this is not needed beyond the notebook.
  The best models were found to be PVLib's Simplified Solis model (`simplified_solis`) and the UK Met Office model 
  (`ukmo_seamless`), respectively.

The notebook `ghi-analysis.ipynb` plots `f_dni_to_ghi` in addition to 
`DNI_modelled`, `DNI_actual`, `GHI_modelled` and `GHI_actual` for the four days of interest. It also 
calculates and plots Plane-Of-Array (POA) irradiance for a tilted solar panel configuration, which represents
the total irradiance (direct and diffuse) received by a solar panel at a specific tilt and azimuth angle.
