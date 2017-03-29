from __future__ import division, unicode_literals

from pymatgen.io.vasp.outputs import Incar, Outcar, Kpoints, Potcar, Locpot

from fireworks.core.firework import FireTaskBase, FWAction, Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from fireworks import explicit_serialize

from custodian.custodian import Custodian

from pymongo import MongoClient
from monty.json import MontyDecoder
import os
from matgendb import QueryEngine


def WorkFunctionWorkFlow(mpids, job, db_credentials, handlers=[],
                         debug=False, collection="surface_tasks", scratch_dir="", launchpad_dir=""):

    conn = MongoClient(host=db_credentials["host"],
                       port=db_credentials["port"])
    db = conn.get_database(db_credentials["database"])
    db.authenticate(db_credentials["user"],
                    db_credentials["password"])
    surface_properties = db["surface_properties"]
    db_credentials["collection"] = collection

    qe = QueryEngine(**db_credentials)

    cwd = os.getcwd()
    scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir
    cust_params = {"scratch_dir": os.path.join(scratch_dir, os.environ["USER"]),
                   "jobs": job,
                   "handlers": handlers,
                   "max_errors": 10}  # will return a list of jobs
    # instead of just being one job
    fws, fw_ids = [], []

    for mpid in mpids:

        print(mpid)
        surface_entry = surface_properties.find_one({"material_id": mpid})
        for surface in surface_entry["surfaces"]:
            if "work_function" not in surface.keys():

                task = surface["tasks"]["slab"]
                entry = qe.get_entries({"task_id": task},
                                       optional_data=["calculations",
                                                      "calculation_name",
                                                      "shift",
                                                      "miller_index"],
                                       inc_structure="Final")[0]

                poscar = entry.structure
                hkl = surface["miller_index"]
                folder = "%s_%s_slab_s10v10_%s%s%s_shift%s" \
                         % (poscar[0].species_string,
                            mpid, hkl[0], hkl[1], hkl[2],
                            entry.data["shift"])
                if surface["is_reconstructed"]:
                    folder = folder + "recon"

                if len(entry.data["calculations"]) == 2:
                    relax2 = entry.data["calculations"][1]
                    incar = relax2["input"]["incar"]
                    poscar = entry.structure

                    os.mkdir(folder)
                    incar.update({"NSW": 0, "IBRION": -1, "LVTOT": True, "EDIFF": 0.0001})

                    incar = Incar.from_dict(incar)
                    incar.write_file(os.path.join(cwd, folder, "INCAR"))

                    kpoints = Kpoints.from_dict(relax2["input"]["kpoints"])
                    kpoints.write_file(os.path.join(cwd, folder, "KPOINTS"))
                    poscar.to("POSCAR", os.path.join(cwd, folder, "POSCAR"))
                    potcar = Potcar(symbols=[poscar[0].species_string],
                                    functional="PBE")
                    potcar.write_file(os.path.join(cwd, folder, "POTCAR"))

                    tasks = [RunCustodianTask(cwd=cwd, folder=folder, debug=debug,
                                              custodian_params=cust_params),
                             InsertTask(cwd=cwd, folder=folder, mpid=mpid,
                                        debug=debug, miller_index=hkl, 
                                        db_credentials=db_credentials)]

                    fw = Firework(tasks, name=folder)
                    fw_ids.append(fw.fw_id)
                    fws.append(fw)

    wf = Workflow(fws, name='Workfunction Calculations')
    launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                 launchpad_dir,
                                                 "my_launchpad.yaml"))
    launchpad.add_wf(wf)


@explicit_serialize
class RunCustodianTask(FireTaskBase):
    """
        Adds dynamicism to the workflow by creating addition Fireworks for each
        termination of a slab or just one slab with shift=0. First the vasp
        inputs of a slab is created, then the Firework for that specific slab
        is made with a RunCustodianTask and a VaspSlabDBInsertTask
    """
    required_params = ["folder", "cwd", "custodian_params", "debug"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        folder = dec.process_decoded(self['folder'])
        cwd = dec.process_decoded(self['cwd'])
        debug = dec.process_decoded(self['debug'])

        if not debug:
            # Change to the directory with the vasp inputs to run custodian
            os.chdir(os.path.join(cwd, folder))

            fw_env = fw_spec.get("_fw_env", {})
            custodian_params = self.get("custodian_params", {})

            # Get the scratch directory
            if fw_env.get('scratch_root'):
                custodian_params['scratch_dir'] = os.path.expandvars(
                    fw_env['scratch_root'])

            c = Custodian(gzipped_output=True, **custodian_params)

            output = c.run()
            return FWAction(stored_data=output)


@explicit_serialize
class InsertTask(FireTaskBase):
    """
        Adds dynamicism to the workflow by creating addition Fireworks for each
        termination of a slab or just one slab with shift=0. First the vasp
        inputs of a slab is created, then the Firework for that specific slab
        is made with a RunCustodianTask and a VaspSlabDBInsertTask
    """
    required_params = ["folder", "cwd", "mpid", "debug",
                       "miller_index", "db_credentials"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        folder = dec.process_decoded(self['folder'])
        cwd = dec.process_decoded(self['cwd'])
        mpid = dec.process_decoded(self['mpid'])
        debug = dec.process_decoded(self['debug'])
        miller_index = dec.process_decoded(self['miller_index'])
        db_credentials = dec.process_decoded(self['db_credentials'])

        conn = MongoClient(host=db_credentials["host"],
                           port=db_credentials["port"])
        db = conn.get_database(db_credentials["database"])
        db.authenticate(db_credentials["user"],
                        db_credentials["password"])
        surface_properties = db["surface_properties"]

        if not debug:

            surface_entry = surface_properties.find_one({"material_id": mpid})
            surfaces = surface_entry["surfaces"]
            update_surfaces = []
            for surface in surfaces:
                if miller_index == surface["miller_index"]:
                    locpot = Locpot.from_file(os.path.join(cwd, folder, "LOCPOT.relax1.gz"))
                    loc = locpot.get_average_along_axis(2)
                    evac = max(loc)
                    outcar = Outcar(os.path.join(folder, "OUTCAR.relax1.gz"))
                    efermi = outcar.efermi
                    surface["work_function"] = evac - efermi
                update_surfaces.append(surface)

            surface_properties.update_one({"material_id": mpid},
                                          {"$set": {"surfaces": update_surfaces}})
