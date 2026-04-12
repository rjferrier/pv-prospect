- [ ] The CSV filepath/filename resolution still isn't right for weather. Look at the extraction entrypoint. The problem is, for weather, we *always* resolve the entity to a grid point. Actually, for weather, the entity can be a pv system ID or a grid point. So `if pv_system_id is not None: ...` needs to come first in the `if` block (l.111). Please correct this and ensure consistency with the runner and with the transformation pipeline too.
- [ ] Check extraction and transformation are working for both types of entity
- [ ] Continue with Workflow engineering

## 2026-07-04

- [ ] Seem that the transformation pipeline is trying to pick up raw PV data that doesn't exist yet. Should steps be limited to weather in the last manual test?
- [ ] What happened with trying to transform weather-grid raw data?
