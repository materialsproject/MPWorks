
## for Surface Energy Calculation
from __future__ import division, unicode_literals
__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"

import os
from pymongo import MongoClient
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteVaspInputs
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs, SlabGenerator
from pymatgen.io.vaspio_set import MPVaspInputSet, DictVaspInputSet
from pymatgen.core.surface import Slab, SlabGenerator, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester

import os
from pymongo import MongoClient
from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad

from pymatgen import write_structure
from pymatgen.io.smartio import CifParser
from matgendb import QueryEngine

class SurfaceWorkflowManager(object):

    # use mpid list instead of list of elements later?

    def __init__(self, max_index, api_key, list_of_elements,
                 indices_dict=None, list_of_indices=None, max_normal_search=False,
                 host=None, port=None, user=None,
                 password=None, database=None, symprec=0.001,
                 angle_tolerance=5, consider_term=False):

        unit_cells_dict = {}
        all_slabs_dict = {}

        vaspdbinsert_params = {'host': host,
                               'port': port, 'user': user,
                               'password': password,
                               'database': database,
                               'collection': "Surface_Calculations"}

        elements = [key for key in indices_dict.keys()] \
            if indices_dict else elements = list_of_elements

        for el in elements:

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
            spa = SpacegroupAnalyzer(prim_unit_cell, symprec=symprec,
                                     angle_tolerance=angle_tolerance)
            conv_unit_cell = spa.get_conventional_standard_structure()
            unit_cells[el] = conv_unit_cell

            if max_normal_search:
                max_norm=max_index
            else: max_norm=None

            if indices_dict:
                list_of_slabs = [SlabGenerator(conv_unit_cell, mill, 10, 10,
                                               max_normal_search=max(mill)).get_slabs()
                                 for mill in indices_dict[el]]

            elif list_of_indices:
                list_of_slabs = [SlabGenerator(conv_unit_cell, mill, 10, 10,
                                               max_normal_search=max(mill)).get_slabs()
                                 for mill in list_of_indices]


            else:
                list_of_slabs = generate_all_slabs(conv_unit_cell, max_index, 10,
                                                   10, max_normal_search=max_norm)

            if not consider_term:
                # Don't care about terminations, get rid of slabs
                # with duplicate miller indices, for studying
                # simple structures such as transition metals
                list_miller=[]
                unique_slabs=[]
                for i, slab in enumerate(list_of_slabs):
                    if slab.miller_index not in list_miller:
                        list_miller.append(slab.miller_index)
                        unique_slabs.append(slab)
            else:
                list_miller=[slab.miller_index for slab in list_of_slabs]

            list_of_slabs = unique_slabs[:]
            slabs_dict[el] = list_of_slabs

        self.api_key = api_key
        self.vaspdbinsert_params = vaspdbinsert_params
        self.symprec = symprec
        self.angle_tolerance = angle_tolerance
        self.unit_cells_dict
        self.slabs_dict

    def create_workflow(self, launchpad_dir="",
                        k_product=50, cwd=os.getcwd(),
                        job=VaspJob(["mpirun", "-n", "16", "vasp"])):

        launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                     launchpad_dir,
                                                     "my_launchpad.yaml"))
        launchpad.reset('', require_password=False)

        cust_params = {"custodian_params":
                           {"scratch_dir":
                                os.path.join("/global/scratch2/sd/",
                                             os.environ["USER"])},
                       "jobs": job}

        fws=[]
        for key in self.slabs_dict.keys():
            print key
            for slab in self.slabs_dict[key]:

                miller_index=slab.miller_index
                print miller_index

                surface_area = slab.surface_area

                vaspdbinsert_parameters = self.vaspdbinsert_params.copy()
                vaspdbinsert_parameters['miller_index'] = miller_index

                folderbulk = '/%s_%s_k%s_%s%s%s' %(slab.formula, 'bulk', k_product,
                                                  str(miller_index[0]),
                                                  str(miller_index[1]),
                                                  str(miller_index[2]))
                folderslab = folderbulk.replace('bulk', 'slab')

                fw = Firework([WriteVaspInputs(slab=slab,
                                               folder=cwd+folderbulk,
                                               user_incar_settings={'MAGMOM': {'Fe': 7}}),
                               RunCustodianTask(dir=cwd+folderbulk, **cust_params),
                               VaspDBInsertTask(struct_type="oriented_unit_cell",
                                                loc=cwd+folderbulk,
                                                **vaspdbinsert_parameters),
                               WriteVaspInputs(slab=slab,
                                               folder=cwd+folderslab, bulk=False,
                                               user_incar_settings={'MAGMOM': {'Fe': 7}}),
                               RunCustodianTask(dir=cwd+folderslab, **cust_params),
                               VaspDBInsertTask(struct_type="slab_cell",
                                                loc=cwd+folderslab,
                                                surface_area=surface_area,
                                                **vaspdbinsert_parameters)])

                fws.append(fw)
        wf = Workflow(fws, name="surface_calculations")
        launchpad.add_wf(wf)


    def get_energy_and_wulff(self):

        qe = QueryEngine(collection='Surface_Calculations',
                         **self.vaspdbinsert_params)

        optional_data = ["chemsys", "surface_area", "nsites"
                         "structure_type", "miller_index"]

        to_Jperm2 = 16.0217656
        wulffshapes = {}
        surface_energies = {}

        for key in self.slabs_dict.keys():
            e_surf_list = []
            se_dict = {}
            miller_list = []

            for slab in self.slabs_dict[key]:

                miller_index = slab.miller_index

                slab_criteria = {'chemsys':key,
                                 'structure_type': 'slab_cell',
                                 'miller_index': miller_index}
                unit_criteria = {'chemsys':key,
                                 'structure_type': 'oriented_unit_cell',
                                 'miller_index': miller_index}

                slab_entry = qe.get_entries(slab_criteria, optional_data=optional_data)
                oriented_ucell_entry = qe.get_entries(unit_criteria, optional_data=optional_data)

                slabE = slab_entry.uncorrected_energy
                bulkE = oriented_ucell_entry.energy_per_atom*slab_entry.data['nsites']
                area = slab_entry.data['surface_area']
                surface_energy = ((slabE-bulkE)/(2*area))*to_Jperm2

                e_surf_list.append(surface_energy)
                se_dict[str(miller_index)] = surface_energy
                miller_list.append(miller_index)

            wulffshapes[el] = wulff_3d(self.unitcell[el], miller_list, e_surf_list)
            surface_energies[el] = se_dict

        return wulffshapes, surface_energies