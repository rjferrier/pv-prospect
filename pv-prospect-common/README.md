# PV Prospect Common

Shared common models and utilities for PV Prospect projects.

This package contains common data structures used across multiple PV Prospect components:
- Domain models (PVSite, Location, DateRange, etc.)
- PV Site repository functionality

## Installation

This package is designed to be used as a local dependency in a monorepo structure.

In other projects' `pyproject.toml`:

```toml
[tool.poetry.dependencies]
pv-prospect-common = { path = "../pv-prospect-common", develop = true }
```

## Usage

```python
from pv_prospect.common import PVSite, Location, DateRange, Period
from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_pv_site_by_system_id
```
