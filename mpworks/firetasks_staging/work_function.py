from __future__ import division, unicode_literals

from pymatgen.io.vasp.outputs import Incar, Outcar, Kpoints, Potcar, Locpot
from pymatgen.io.vasp.sets import MVLSlabSet

from fireworks.core.firework import FireTaskBase, FWAction, Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from fireworks import explicit_serialize

from custodian.custodian import Custodian
from custodian.vasp.jobs import VaspJob

from pymongo import MongoClient
from monty.json import MontyDecoder
import os
import shutil
from matgendb import QueryEngine


def WorkFunctionWorkFlow(mpids, db_credentials, handlers=[],
                         debug=False, scratch_dir="", launchpad_dir=""):

    conn = MongoClient(host=db_credentials["host"],
                       port=db_credentials["port"])
    db = conn.get_database(db_credentials["database"])
    db.authenticate(db_credentials["user"],
                    db_credentials["password"])
    surface_properties = db["surface_properties"]

    qe = QueryEngine(**db_credentials)

    cwd = os.getcwd()
    scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir
    cust_params = {"scratch_dir": os.path.join(scratch_dir, os.environ["USER"]),
                   "handlers": handlers, "max_errors": 10}
    fws, fw_ids = [], []
    
    if not os.path.isdir("complete"):
        os.mkdir("complete")
    
    for mpid in mpids:

        print(mpid)
        surface_entry = surface_properties.find_one({"material_id": mpid})
        if not surface_entry:
            print(surface_entry)
        for surface in surface_entry["surfaces"]:
            if "work_function" not in surface.keys():

                task = surface["tasks"]["slab"]
                entries = qe.get_entries({"task_id": task},
                                       optional_data=["calculations",
                                                      "calculation_name",
                                                      "shift",
                                                      "miller_index"],
                                       inc_structure="Final")
                if not entries:
                    print(task)
                else:
                    entry = entries[0]

                poscar = entry.structure
                hkl = surface["miller_index"]
                folder = "%s_%s_slab_s10v10_" %(poscar[0].species_string, mpid)
                for i in hkl:
                    folder = folder+str(i)
                folder = folder + "_shift%s" %(entry.data["shift"])                      
                if surface["is_reconstructed"]:
                    folder = folder + "recon"

                if len(entry.data["calculations"]) == 2:
                    relax = entry.data["calculations"][1]
                else:
                    relax = entry.data["calculations"][0]
                incar = relax["input"]["incar"]
                poscar = entry.structure

                os.mkdir(folder)
                mplb = MVLSlabSet(poscar, potcar_functional="PBE")
                mplb.write_input(os.path.join(cwd, folder))

                incar.update({"NSW": 0, "IBRION": -1, "LVTOT": True, "EDIFF": 0.0001})
                incar = Incar.from_dict(incar)
                incar.write_file(os.path.join(cwd, folder, "INCAR"))

                kpoints = Kpoints.from_dict(relax["input"]["kpoints"])
                kpoints.write_file(os.path.join(cwd, folder, "KPOINTS"))
                poscar.to("POSCAR", os.path.join(cwd, folder, "POSCAR"))

                tasks = [RunCustodianTask(cwd=cwd, folder=folder, debug=debug,
                                          custodian_params=cust_params),
                         InsertTask(cwd=cwd, folder=folder, debug=debug,
                                    db_credentials=db_credentials,
                                    task_id=task)]

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
            job = VaspJob(["mpirun", "-np", "16",
                           "/opt/vasp/5.2.12/openmpi_ib/bin/vasp"],
                          auto_npar=False, copy_magmom=True, suffix=".relax1")
            c = Custodian(jobs=[job], gzipped_output=True, **custodian_params)

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
    required_params = ["folder", "cwd", "debug",
                       "task_id", "db_credentials"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        folder = dec.process_decoded(self['folder'])
        cwd = dec.process_decoded(self['cwd'])
        debug = dec.process_decoded(self['debug'])
        task_id = dec.process_decoded(self['task_id'])
        db_credentials = dec.process_decoded(self['db_credentials'])

        conn = MongoClient(host=db_credentials["host"],
                           port=db_credentials["port"])
        db = conn.get_database(db_credentials["database"])
        db.authenticate(db_credentials["user"],
                        db_credentials["password"])
        surface_tasks = db["surface_tasks"]

        if not debug:

            locpot = Locpot.from_file(os.path.join(cwd, folder, "LOCPOT.gz"))
            loc = locpot.get_average_along_axis(2)
            efermi = Outcar(os.path.join(cwd, folder, "OUTCAR.relax1.gz")).efermi
            surface_tasks.update_one({"task_id": task_id},
                                     {"$set": {"local_potential_along_c": loc,
                                               "efermi": efermi}})
