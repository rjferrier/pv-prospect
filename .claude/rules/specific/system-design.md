
## Consistency

When proposing a fix or improvement to one part of the system, also assess
whether other parts that *share the same pattern* need the same treatment.
If they do, apply the change everywhere the pattern fits — not just where
the immediate symptom appeared. And when the same change would be made
across multiple components, extract the common code into a shared library
(e.g. `pv-prospect-etl` for storage/orchestration concerns) rather than
duplicating it per package.

**Why:** A system whose components implement the same idea slightly
differently is hard to reason about; one-off fixes accrete into a hodgepodge
of near-identical-but-not-quite patterns that look intentional. Applying
fixes consistently — and refactoring out the duplication that triggers them
— is what keeps the codebase coherent over time.

**How to apply:** Before reporting a proposal as done, ask explicitly: "what
other components match this pattern?" If any do, either include them in the
same change (preferred) or call them out as deliberately excluded with a
reason. When proposing the fix, mention the cross-component scope up front
so the user can decide on the breadth of the work.

**Example that triggered this:** Fan-out elimination via in-memory
collectors was applied to the transformation backfill alone (commit
`e227c6b`), even though the extraction backfills (WGB in particular) shared
the same single-process-with-fan-out pattern. The right call would have
been to apply it on both sides and put the collector wiring in the shared
ETL library.
