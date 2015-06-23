import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks import ScriptTask
from fireworks.core.launchpad import LaunchPad
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteSurfVaspInput, SimplerCustodianTask
from custodian.vasp.jobs import VaspJob


import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks.core.launchpad import LaunchPad


def create_surface_workflows(miller_index, api_key, element, k_product=50):

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




##########################TEST WORKFLOW WITH Mo 001 SLAB##########################

launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                              "surf_wf_tests", "my_launchpad.yaml"))
launchpad.reset('', require_password=False)

wf = create_surface_workflows([0,0,1], " mlcC4gtXFVqN9WLv", "Mo")
launchpad.add_wf(wf)
