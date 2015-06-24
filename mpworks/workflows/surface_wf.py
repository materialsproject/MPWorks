import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks import ScriptTask
from fireworks.core.launchpad import LaunchPad
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteSurfVaspInput, SimplerCustodianTask
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs


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



def surface_workflows(max_index, api_key, list_of_elements, k_product=50):

    fws = []
    fw = FireWork([WriteSurfVaspInputs(list_of_elements=list_of_elements,
                                      max_index=max_index,
                                      api_key=api_key)])
    fws.append(fw)

    for dir in os.listdir('.'):

        # need to move inputs to individual scratch directories instead in order to run all calculations in parrallel, or run them one by one?

        cp_inputs = ScriptTask.from_str("cp %s/* ./" %(dir))

        mv_outputs = ScriptTask.from_str("mv CHG CHGCAR DOSCAR EIGENVAL "
                                     "IBZKPT OSZICAR OUTCAR PCDAT PROCAR "
                                     "vasprun.xml WAVECAR XDATCAR CONTCAR %s/"
                                     %(dir))
        fw = FireWork([cp_inputs, SimplerCustodianTask(), mv_outputs])
        fws.append(fw)

    wf = Workflow(fws, name="3D Metal Surface Energy Workflow")



    return wf



##########################TEST WORKFLOW WITH Mo 001 SLAB##########################

launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                              "surf_wf_tests", "my_launchpad.yaml"))
launchpad.reset('', require_password=False)

wf = create_surface_workflows([0,0,1], " mlcC4gtXFVqN9WLv", "Mo")
launchpad.add_wf(wf)

##########################TEST WORKFLOW WITH ALL 3D TMs##########################

# launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
#                                               "surf_wf_tests", "my_launchpad.yaml"))
# launchpad.reset('', require_password=False)
#
# tms_3d = ['Sc', 'Ti', 'V', 'Cr', 'Mn', 'Fe', 'Co', 'Ni', 'Cu']
# wf = create_surface_workflows(2, " mlcC4gtXFVqN9WLv", tms_3d)
# launchpad.add_wf(wf)