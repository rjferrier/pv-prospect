
- [ ] It turns out the Open-Meteo data is off by one hour. Compare the 
    `direct_normal_irradiance_ukmo_seamless` values on 2025-03-30: there is 
    a jump up to 846.1 at 13:00 in the data I previously pulled, 
    but checking the API again, it seems the jump actually happens at 12:00.
    So I need to check and regenerate some or all of the data :/
- [ ] Daylight savings is accounted for in PVOutput API, but not the Open-Meteo API.
    The datetimes need to be adjusted accordingly.
