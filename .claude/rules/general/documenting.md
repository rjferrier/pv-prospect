## When to Document

The other rules specify when to document changes.


## What to Document

Include, if appropriate:

* Context: _why_ was the change done, or why does it differ to other parts of the
  project?
* High level structures and interactions that are not obvious or documented within
  a single scope (module, class, function).
* Relevant nomenclature / terminology.


## Where to Document

Locate the place for adding documentation thus:

  - If the change (1) spans multiple sub-projects (packages/supporting libraries)
    and (2) either (2.1) describes the system design or behaviour at a high level
    or (2.2) has external implications (e.g. affects the contents of a bucket in
    cloud storage), documentation should go in the top-level README.md.
  - If the change (1) concerns just one sub-project and (2) either (2.1) describes
    the system design or behaviour at a high level or (2.2) has external
    implications, then document it in the README.md of the appropriate sub-project.
  - Otherwise, if the change spans multiple sub-projects, document it in an
    appropriate `.md` file in the top-level `doc/` directory. (Create the file if
    necessary.)
  - Otherwise, if the change (1) concerns just one sub-project, document it in an
    appropriate .md file in the `doc/` directory of the appropriate sub-project.
    (Create the directory and file if necessary.)

Check all such documentation when modifying or removing functionality, and update it
as appropriate.
