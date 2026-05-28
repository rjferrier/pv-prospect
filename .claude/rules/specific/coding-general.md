## Testing

When adding or modifying functionality, be sure to add and/or update the automated
tests, whether they are unit, integration, system or acceptance tests.
You may create a new unit test module when adding a new unit to be tested.
However, do not automatically create new modules for higher-level tests unless
instructed.


## Refactoring

Refactor whenever appropriate. Is the same code used in two places, and do those places
share a common library or modules? Then extract the code to the library or one of the
shared modules, or consider creating a new shared module.

As a rule,
* If the opportunity for refactoring presents itself within a single scope (module,
  class, function) that is being added or modified, do the refactoring in the same
  commit that adds or modifies that scope.
* If refactoring becomes appropriate but it affects multiple other existing scopes,
  do the refactoring in a separate commit to the functional changes. Multiple such
  refactorings may be grouped together in the same commit as part of the same
  project-level task.
* A grey area: what if refactoring affects other classes or functions within the same
  module, but the actual refactoring change is very small? Then it may be acceptable
  to do the refactoring in the same commit as the functional changes to that module.
  Check with the user if in doubt.


## Documenting

As code is added/updated/deleted, make sure you add/update/delete corresponding
documentation. For more rules about documenting, see documenting.md.
