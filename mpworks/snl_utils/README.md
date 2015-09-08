# snl_utils

This package is poorly-named, but contains MP extensions to SNL that are needed for duplicate checking and database storage of SNL.

This includes:
- MPSNL, which adds snl_id and spacegroup info to an SNL
- SNLGroup, which represents a "material" and can have several associated SNL
- Routines for adding an SNL into the database, assigning an SNLGroup, etc.