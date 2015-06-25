
## for Surface Energy Calculation
from __future__ import division, unicode_literals
__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"

import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks import ScriptTask
from fireworks.core.launchpad import LaunchPad
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteSurfVaspInput, SimplerCustodianTask, \
    WriteSlabVaspInputs, WriteUnitCellVaspInputs
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs


import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks.core.launchpad import LaunchPad


def surface_workflows(miller_index, api_key, element, k_product=50):

    cpbulk = ScriptTask.from_str("cp %s_ucell_k%s_%s%s%s/* ./" %(element, k_product,
                                                                 str(miller_index[0]),
                                                                 str(miller_index[1]),
                                                                 str(miller_index[2])))
    cpslab = ScriptTask.from_str("cp %s_scell_k%s_%s%s%s/* ./" %(element, k_product,
                                                                 str(miller_index[0]),
                                                                 str(miller_index[1]),
                                                                 str(miller_index[2])))

    mvbulk = ScriptTask.from_str("mv CHG CHGCAR DOSCAR EIGENVAL "
                                 "IBZKPT OSZICAR OUTCAR PCDAT PROCAR "
                                 "vasprun.xml WAVECAR XDATCAR CONTCAR %s_ucell_k%s_%s%s%s/"
                                 %(element, k_product,
                                   str(miller_index[0]),
                                   str(miller_index[1]),
                                   str(miller_index[2])))
    mvslab = ScriptTask.from_str("mv CHG CHGCAR DOSCAR EIGENVAL "
                                 "IBZKPT OSZICAR OUTCAR PCDAT PROCAR "
                                 "vasprun.xml WAVECAR XDATCAR CONTCAR %s_scell_k%s_%s%s%s/"
                                 %(element, k_product,
                                   str(miller_index[0]),
                                   str(miller_index[1]),
                                   str(miller_index[2])))

    fws = []
    # job = VaspJob(["aprun", "-n", "48", "vasp"])

    fw = FireWork([WriteSurfVaspInput(element=element,
                                      miller_index=miller_index,
                                      api_key=api_key), cpbulk,
                  SimplerCustodianTask()])
    fws.append(fw)

    wf = Workflow(fws, name="3D Metal Surface Energy Workflow")

    return wf








def create_surface_workflows(max_index, api_key, list_of_elements, k_product=50,
                      host=None, port=None, user=None, password=None, database=None):

    fws = []

    fw = FireWork([WriteUnitCellVaspInputs(list_of_elements=list_of_elements,
                                      max_index=max_index,
                                      api_key=api_key)])
    fws.append(fw)

    for dir in os.listdir('.'):

        # need to move inputs to individual scratch directories instead in order to run all calculations in parrallel, or run them one by one?

        outputs = "mv INCAR POSCAR POTCAR KPOINTS " \
                  "CHG CHGCAR DOSCAR EIGENVAL IBZKPT " \
                  "OSZICAR OUTCAR PCDAT PROCAR " \
                  "vasprun.xml WAVECAR XDATCAR CONTCAR %s/"

        fw = FireWork([ScriptTask.from_str("cp %s/* ./" %(dir)),
                       SimplerCustodianTask(),
                       ScriptTask.from_str(outputs %(dir)),
                       VaspDBInsertTask(host=host, port=port, user=user,
                                        password=password, database=database,
                                        collection="Surface Calculations",
                                        struct_type="oriented unit cell",
                                        miller_index=dir[-3:], bulk=True),
                       WriteSlabVaspInputs(dir=dir),
                       ScriptTask.from_str("cp %s/* ./" %(dir.replace("ucell", "scell"))),
                       SimplerCustodianTask(),
                       ScriptTask.from_str(outputs %(dir.replace("ucell", "scell"))),
                       VaspDBInsertTask(host=host, port=port, user=user,
                                        password=password, database=database,
                                        collection="Surface Calculations",
                                        struct_type="oriented unit cell",
                                        miller_index=dir[-3:], bulk=True)])
        fws.append(fw)

    wf = Workflow(fws, name="3d Metal Surface Energy Workflow")



    return wf



