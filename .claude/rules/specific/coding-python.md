---
paths:
  - "**/*.py"
---

## Code Style

General code style (including Markdown):
- 88-char line length

Python style:
- Python 3.12, single quotes (see `ruff.toml` at repo root).
- All functions must be typed (`--disallow-untyped-defs`).
- `__init__.py` files are excluded from mypy.
- Ruff rules: `E4`, `E7`, `E9`, `F`, `I` (isort), `B` (bugbear),
  `C4` (comprehensions).
- Imports should go at the top of the module, not inlined in module members

### Naming Members

Name variables holding domain objects after their class in snake_case. For example: `grid_point: GridPoint`, `pv_site: PVSite`, `date_range: DateRange`.

**Why:** Consistent, predictable naming — the type tells you what it is, and the name confirms it.

**How to apply:** Whenever introducing a variable for a domain object (parameter, local variable, fixture), default to the snake_case form of its class name unless there is a specific reason to distinguish multiple instances.

### Package Imports

Module members used by **other packages in production code** must be re-exported through the **immediate owning package's** `__init__.py`. Imports must reflect the actual package hierarchy — consumers must not flatten upward to a parent package.

**Member only used by tests:** Do NOT add to `__init__.py`. Tests import directly from the module path (`from fee.fi.foo.bar import baz`). This is the one exception where module-level imports are correct.

**Member used cross-package in production:** Add to owning package's `__init__.py`. Consumers import from the package (`from fee.fi.foo import baz`). Never import from the module within it (`from fee.fi.foo.bar import baz`).

**Import hierarchy rule:**
- ✓ `from fee.fi.foo import baz` — correct; imports from the package that owns `bar`
- ✗ `from fee.fi.foo.bar import baz` — wrong; exposes internal module (unless test-only exception applies)
- ✗ `from fee.fi import baz` — wrong; flattens up to parent package
- ✗ `from fee import baz` — wrong; flattens up to grandparent package

**Why:** Internal module layout is an implementation detail. The package hierarchy is meaningful and visible in import paths. Test-only members shouldn't pollute the package's public API.

**How to apply:**
1. Determine if the member is used only in tests or also in production code across packages.
2. If test-only → leave at module level, test imports from module path.
3. If cross-package production use → add to owning package's `__init__.py`, update all production imports to use package path.
4. Intra-package imports (within the same package) are unaffected by this rule.


## Principles & Practices

### Member Visibility and Unit Testing

Only test public members. If a private member deserves dedicated unit tests
(significant combinatorial test situation), publicise it by removing the leading
underscore. If its usages don't lead to a combinatorial explosion, leave it private
— it will be covered indirectly through callers.

**Why:** Keeps tests coupled to the public API, avoids testing implementation details that
 don't warrant it.

**How to apply:** Before writing tests for a `_private` function, assess whether its
logic is combinatorially complex. If yes, rename to public and test directly. If no,
leave private and test through its callers.


### Function Purity and Unit Testing

As a rule, non-trivial logic should exist in pure functions or methods, which means
unit tests can be written without the need to create patches. The existence of patches
is therefore considered a code smell which necessitates one of two things: (1) the
logic should be moved to a pure function, or the existing function be made pure, and
collaborating objects that would otherwise make the function impure should be passed
at an appropriately high (i.e. factory or orchestration) level. At that
level, unit tests may be omitted; integration or acceptance tests would be more
appropriate. (2) If the pure logic is actually trivial, e.g. there is no conditional
branching or other such constructs, it may be overkill to try and extract it and create
a unit test for it. The unit test in this case is not pulling its weight and may simply
be omitted. (Again, integration or acceptance tests should cover the functionality.)

## Structuring Tests

Write unit tests as top-level `def test_*()` functions, never as methods inside
a `class Test*`. When a module under test has multiple members being tested,
give each member its own test module, grouped under a package named after the
module under test. Use pytest fixtures if necessary.

**Why:** Granular, well-organised, non-class-based test structure.

**How to apply:** E.g. for `config_parser.py` with `_merge_dicts` and
`load_config_tree`, create `tests/unit/config_parser/__init__.py`,
`tests/unit/config_parser/test_merge_dicts.py`, and
`tests/unit/config_parser/test_load_config_tree.py`, each containing plain
`def test_*()` functions.
