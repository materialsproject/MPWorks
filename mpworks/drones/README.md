# Drones package

 The MPVaspDrone is currently used by the production workflow.

The drones package is an extension of the pymatgen-db drone, which converts a VASP directory into a database dictionary. The MPVaspDrone adds a "post_process" method and modifies some of the default drone behavior. It would be better if this could extend the existing drone rather than repeat a lot of the pymatgen-db drone, but it was not workable at the time of its creation.

For example, the signal detectors help tag extra things that have might gone wrong with the run, and put it in the key analysis.signals and analysis.critical_signals.

Another thing the custom drone does is SNL management. In particular, for structure optimizations it adds a new SNL to the SNL database (the newly optimized structure). For static runs (where the structure doesn't change), a new SNL is not added. The packages also add keys like "snlgroup_changed" which check whether the new and old SNL match after the relaxation run.