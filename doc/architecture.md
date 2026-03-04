# Architecture

![Architecture](/doc/architecture.png)

## Flows

| Key | Description                 | When it happens       |
|-----|-----------------------------|-----------------------|
| EW  | Weather data extraction     | Daily                 |
| EP  | PV data extraction          | Daily                 |
| TW  | Weather data transformation | When EW and EP finish |
| TP  | PV data transformation      | When EW and EP finish |
| L   | App data loading            | When App starts up    |
| V   | Data/model versioning       | Weekly                |
| MW  | Weather model training      | When V finishes       |
| MP  | PV model training           | When V finishes       |
