import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks import ScriptTask
from fireworks.core.launchpad import LaunchPad
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from mpworks.firetasks.surface_tasks import RunCustodianTask, VaspDBInsertTask, WriteSurfVaspInput
from custodian.vasp.jobs import VaspJob


import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks.core.launchpad import LaunchPad

fws = []
bulkJobs = VaspJob.double_relaxation_run(["aprun", "-n", "48", "vasp"])
fw = FireWork([RunCustodianTask({'job': bulkJobs})])

fws.append(fw)

wf = Workflow(fws, name="unitcell relaxation")

launchpad = LaunchPad(host='ds043497.mongolab.com', port=43497, name='rit001_db',
                      username='rit001', password='fYr4ni!8', strm_lvl='INFO',
                      user_indices=[], wf_user_indices=[])

launchpad.reset('', require_password=False)
launchpad.add_wf(fw)



def create_surface_workflows(miller_index, api_key, element, k_product=50):

    t1 = ("cd ~/.fireworks/%s_ucell_k%s_%s" %element %k_product %miller_index)
    t2 = ("cd ~/.fireworks/%s_scell_k%s_%s" %element %k_product %miller_index)
    fws = []
    bulkJobs = VaspJob.double_relaxation_run(["aprun", "-n", "48", "vasp"])
    slabJobs = VaspJob.run(["aprun", "-n", "48", "vasp"])


    fw = FireWork([WriteSurfVaspInput(element=element,
                                      miller_index=miller_index,
                                      api_key=api_key)])
    fws.append(fw)

    fw = FireWork([t1, RunCustodianTask({'job': bulkJobs}),
                   VaspDBInsertTask(database="debug",
                                    collection="Surface_calculations",
                                    host="mavrldb.ucsd.edu",
                                    port=27017, user="hulk",
                                    password="7oZEAdL,zW4WNXG",
                                    mapi_key="xNebFpxTfLhTnnIH",
                                    miller_index=str(miller_index,),
                                    struct_type="bulk")])
    fws.append(fw)

    fw = FireWork([t2, RunCustodianTask({'job': slabJobs}),
                   VaspDBInsertTask(database="debug",
                                    collection="Surface_calculations",
                                    host="mavrldb.ucsd.edu",
                                    port=27017, user="hulk",
                                    password="7oZEAdL,zW4WNXG",
                                    mapi_key="xNebFpxTfLhTnnIH",
                                    miller_index=str(miller_index,),
                                    struct_type="slab")])
    fws.append(fw)

    wf = Workflow(fws, name="3D Metal Surface Energy Workflow")
    return wf

def mo001_calculation():
    launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                              ".fireworks", "my_launchpad.yaml"))
    launchpad.reset('', require_password=False)

