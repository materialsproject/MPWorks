
## for Surface Energy Calculation
from __future__ import division, unicode_literals
__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"


import os

from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteVaspInputs
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs, SlabGenerator
from pymatgen.core.surface import SlabGenerator, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from matgendb import QueryEngine


def termination(consider_term, list_of_slabs, el):
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

    return list_of_slabs

class SurfaceWorkflowManager(object):

    def __init__(self, api_key, list_of_elements=[], indices_dict=None,
                 host=None, port=None, user=None, password=None, consider_term=False,
                 symprec=0.001, angle_tolerance=5, database=None):

        """
        Initializes the workflow manager by taking in a combination of compounds and miller
        indices. Allows for three ways to designate a combination of compounds and miller
        indices to generate slabs based on its class methods.

        Args:
            api_key (str): A String API key for accessing the MaterialsProject
            list_of_elements ([str, ...]): A list of compounds or elements to create slabs
                from. Must be a string that can be searched for with MPRester. Either
                list_of_elements or indices_dict has to be entered in.
            indices_dict ({element(str): [[h,k,l], ...]}): A dictionary of miller indices
                corresponding to the compound (key) to transform into a list of slabs.
                Either list_of_elements or indices_dict has to be entered in.
            host (str): For database insertion
            port (int): For database insertion
            user (str): For database insertion
            password (str): For database insertion
            consider_term (bool): Whether or not different terminations of a surface will be
                considered when creating slabs.
            symprec (float): See SpaceGroupAnalyzer in analyzer.py
            angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
            database (str): For database insertion
        """

        unit_cells_dict = {}
        vaspdbinsert_params = {'host': host,
                               'port': port, 'user': user,
                               'password': password,
                               'database': database,
                               'collection': "Surface_Calculations"}

        elements = [key for key in indices_dict.keys()] \
            if indices_dict else list_of_elements

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
            print conv_unit_cell
            unit_cells_dict[el] = conv_unit_cell
            print el


        self.api_key = api_key
        self.vaspdbinsert_params = vaspdbinsert_params
        self.symprec = symprec
        self.angle_tolerance = angle_tolerance
        self.unit_cells_dict = unit_cells_dict
        self.indices_dict = indices_dict
        self.elements = elements


    def from_max_index(self, max_index, max_normal_search=False, consider_term=False):

        max_norm=max_index if max_normal_search else None
        slabs_dict = {}
        for el in self.elements:
            # generate_all_slabs() is very slow, especially for Mn
            list_of_slabs = generate_all_slabs(self.unit_cells_dict[el], max_index, 10,
                                               10, max_normal_search=max_norm)

            print 'surface ', el

            slabs_dict[el] = termination(consider_term, list_of_slabs, el)

            print '# ', el

        return CreateSurfaceWorkflow(slabs_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params)


    def from_list_of_indices(self, list_of_indices, max_normal_search=False,
                             consider_term=False):

        slabs_dict = {}
        for el in self.elements:

            list_of_slabs=[]
            for mill in list_of_indices:
                max_norm=max(mill) if max_normal_search else None
                for slab in SlabGenerator(self.unit_cells_dict[el], mill, 10, 10,
                                          max_normal_search=max_norm).get_slabs():
                    list_of_slabs.append(slab)

            slabs_dict[el] = termination(consider_term, list_of_slabs, el)

        return CreateSurfaceWorkflow(slabs_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params)


    def from_indices_dict(self, max_normal_search=False, consider_term=False):

        slabs_dict = {}
        for el in self.elements:

            list_of_slabs=[]
            for mill in self.indices_dict[el]:
                max_norm=max(mill) if max_normal_search else None

                for slabs in SlabGenerator(self.unit_cells_dict[el], mill, 10, 10,
                                           max_normal_search=max_norm).get_slabs():
                    list_of_slabs.append(slabs)

            slabs_dict[el] = termination(consider_term, list_of_slabs, el)

        return CreateSurfaceWorkflow(slabs_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params)


class CreateSurfaceWorkflow(object):

    def __init__(self, slabs_dict, unit_cells_dict, vaspdbinsert_params):

        self.slabs_dict = slabs_dict
        self.unit_cells_dict = unit_cells_dict
        self.vaspdbinsert_params = vaspdbinsert_params

    def launch_workflow(self, launchpad_dir="",
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
                print str(miller_index)

                surface_area = slab.surface_area

                vaspdbinsert_parameters = self.vaspdbinsert_params.copy()
                vaspdbinsert_parameters['miller_index'] = miller_index

                folderbulk = '/%s_%s_k%s_%s%s%s' %(slab.composition.reduced_formula,
                                                   'bulk', k_product,
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

                slab_entry = qe.get_entries(slab_criteria,
                                            optional_data=optional_data)
                oriented_ucell_entry = qe.get_entries(unit_criteria,
                                                      optional_data=optional_data)

                slabE = slab_entry.uncorrected_energy
                bulkE = oriented_ucell_entry.energy_per_atom*slab_entry.data['nsites']
                area = slab_entry.data['surface_area']
                surface_energy = ((slabE-bulkE)/(2*area))*to_Jperm2

                e_surf_list.append(surface_energy)
                se_dict[str(miller_index)] = surface_energy
                miller_list.append(miller_index)

            wulffshapes[el] = wulff_3d(self.unit_cells_dict[el], miller_list, e_surf_list)
            surface_energies[el] = se_dict

        return wulffshapes, surface_energies