
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
    VaspDBInsertTask, WriteSurfVaspInput, WriteVaspInputs
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs, SlabGenerator
from pymatgen.io.vaspio_set import MPVaspInputSet, DictVaspInputSet
from pymatgen.core.surface import Slab, SlabGenerator, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester

import os
from pymongo import MongoClient
from fireworks.core.firework import FireWork, Workflow
from fireworks.core.launchpad import LaunchPad


# debug
# debug
# debug
# debug
def surface_workflows(miller_index, api_key, element, k_product=50, symprec=0.001, angle_tolerance=5):

    launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                 "surf_wf_tests",
                                                 "my_launchpad.yaml"))
    launchpad.reset('', require_password=False)

    mprest = MPRester(api_key)
    #first is the lowest energy one
    prim_unit_cell = mprest.get_structures(element)[0]
    spa = SpacegroupAnalyzer(prim_unit_cell,  symprec=symprec,
                             angle_tolerance=angle_tolerance)
    conv_unit_cell = spa.get_conventional_standard_structure()
    slab = SlabGenerator(conv_unit_cell, max_index,
                         10, 10, primitive=False,
                         max_normal_search=max_index)
    slab = slab.get_slab()

    ocwd = os.getcwd()

    folderbulk = '%s_%s_k%s_%s%s%s' %(element, 'bulk', k_product,
                                      str(miller_index[0]),
                                      str(miller_index[1]),
                                      str(miller_index[2]))

    fws = []
    job = "mpirun", "-n", "48", "vasp"

    fw = FireWork([WriteVaspInputs(structure=slab,
                                   folder=ocwd+folderbulk),
                   SimplerCustodianTask(dir=ocwd+folderbulk,
                                        jobs=job)])
    fws.append(fw)

    wf = Workflow(fws, name=folderbulk)

    return wf





































def create_surface_workflows(max_index, api_key, list_of_elements,
                             k_product=50, host=None, port=None,
                             user=None, password=None, database=None, symprec=0.001, angle_tolerance=5):

    launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                 "surf_wf_tests",
                                                 "my_launchpad.yaml"))
    launchpad.reset('', require_password=False)

    for el in list_of_elements:

        """
        element: str, element name of Metal
        miller_index: hkl, e.g. [1, 1, 0]
        api_key: to get access to MP DB
        """
        # This initializes the REST adaptor. Put your own API key in.
        # e.g. MPRester("QMt7nBdIioOVySW2")
        mprest = MPRester(api_key)
        #first is the lowest energy one
        prim_unit_cell = mprest.get_structures(el)[0]
        spa = SpacegroupAnalyzer(prim_unit_cell,  symprec=symprec,
                                 angle_tolerance=angle_tolerance)
        conv_unit_cell = spa.get_conventional_standard_structure()

        list_of_slabs = generate_all_slabs(conv_unit_cell, max_index,
                                           10, 10, primitive=False,
                                           max_normal_search=max_index)


        ocwd = os.getcwd()
        for slab in list_of_slabs:

            fws=[]

            folderbulk = '%s_%s_k%s_%s%s%s' %(slab[0].specie, 'bulk', k_product,
                                              str(miller_index[0]),
                                              str(miller_index[1]),
                                              str(miller_index[2]))
            folderslab = folderbulk.replace('bulk', 'slab')

            fw = FireWork([WriteVaspInputs(structure=slab,
                                           folder=ocwd+folderbulk),
                           CustodianTask(cwd=ocwd+folderbulk),
                           VaspDBInsertTask(host=host, port=port, user=user,
                                            password=password, database=database,
                                            collection="Surface Calculations",
                                            struct_type="oriented unit cell",
                                            miller_index=dir[-3:],
                                            loc=ocwd+folderbulk),
                           WriteVaspInputs(structure=slab,
                                           folder=ocwd+folderslab),
                           CustodianTask(cwd=ocwd+folderslab),
                           VaspDBInsertTask(host=host, port=port, user=user,
                                            password=password, database=database,
                                            collection="Surface Calculations",
                                            struct_type="slab cell",
                                            miller_index=dir[-3:],
                                            loc=ocwd+folderslab)])
            fws.append(fw)
            wf = Workflow(fws, name="%s %s surface calculation" %(el, slab.miller_index))
            launchpad.add_wf(wf)