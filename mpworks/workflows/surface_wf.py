
## for Surface Energy Calculation
from __future__ import division, unicode_literals
__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"


import os

from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspDBInsertTask, WriteSlabVaspInputs, WriteUCVaspInputs
from custodian.vasp.jobs import VaspJob
from pymatgen.core.surface import generate_all_slabs, SlabGenerator, \
    get_symmetrically_distinct_miller_indices
from pymatgen.core.surface import SlabGenerator, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from matgendb import QueryEngine


def termination(list_of_slabs, el):

    list_miller=[]
    unique_slabs=[]
    for i, slab in enumerate(list_of_slabs):
        if slab.miller_index not in list_miller:
            list_miller.append(slab.miller_index)
            unique_slabs.append(slab)


    list_of_slabs = unique_slabs[:]

    return list_of_slabs

class SurfaceWorkflowManager(object):

    def __init__(self, api_key, list_of_elements=[], indices_dict=None,
                 host=None, port=None, user=None, password=None,
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


    def from_max_index(self, max_index, max_normal_search=False, terminations=False):

        max_norm=max_index if max_normal_search else None
        miller_dict = {}
        for el in self.elements:
            # generate_all_slabs() is very slow, especially for Mn
            list_of_indices = \
                get_symmetrically_distinct_miller_indices(self.unit_cells_dict[el],
                                                          max_index)

            print 'surface ', el

            print '# ', el

            miller_dict[el] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params,
                                     max_normal_search=False, terminations=terminations)


    def from_list_of_indices(self, list_of_indices, max_normal_search=False,
                             terminations=False):

        miller_dict = {}
        for el in self.elements:
            miller_dict[el] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params,
                                     max_normal_search=False, terminations=terminations)


    def from_indices_dict(self, max_normal_search=False, terminations=False):



        return CreateSurfaceWorkflow(self.indices_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params,
                                     max_normal_search=False, terminations=terminations)


class CreateSurfaceWorkflow(object):

    def __init__(self, miller_dict, unit_cells_dict, vaspdbinsert_params,
                 terminations=False, max_normal_search=False):

        self.miller_dict = miller_dict
        self.unit_cells_dict = unit_cells_dict
        self.vaspdbinsert_params = vaspdbinsert_params
        self.max_normal_search = max_normal_search
        self.terminations = terminations

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
        for key in self.miller_dict.keys():
            print key
            for miller_index in self.miller_dict[key]:

                print str(miller_index)

                vaspdbinsert_parameters = self.vaspdbinsert_params.copy()
                vaspdbinsert_parameters['miller_index'] = miller_index
                max_norm = max(miller_index) if self.max_normal_search else None

                slab = SlabGenerator(self.unit_cells_dict[key], miller_index,
                                     10, 10, max_normal_search=max_norm)
                oriented_uc = slab.oriented_unit_cell

                folderbulk = '/%s_%s_k%s_%s%s%s' %(oriented_uc.composition.reduced_formula,
                                                   'bulk', k_product,
                                                   str(miller_index[0]),
                                                   str(miller_index[1]),
                                                   str(miller_index[2]))

                fw = Firework([WriteUCVaspInputs(oriented_ucell=oriented_uc,
                                               folder=cwd+folderbulk,
                                               user_incar_settings={'MAGMOM': {'Fe': 7}}),
                               RunCustodianTask(dir=cwd+folderbulk, **cust_params),
                               VaspDBInsertTask(struct_type="oriented_unit_cell",
                                                loc=cwd+folderbulk,
                                                **vaspdbinsert_parameters),
                               WriteSlabVaspInputs(folder=cwd+folderbulk,
                                                   user_incar_settings={'MAGMOM': {'Fe': 7}},
                                                   terminations=self.terminations)])

                fws.append(fw)
        wf = Workflow(fws, name="surface_calculations")
        launchpad.add_wf(wf)


    def get_energy_and_wulff(self):

        qe = QueryEngine(**self.vaspdbinsert_params)

        optional_data = ["chemsys", "surface_area", "nsites"
                         "structure_type", "miller_index"]

        to_Jperm2 = 16.0217656
        wulffshapes = {}
        surface_energies = {}

        for key in self.miller_dict.keys():
            e_surf_list = []
            se_dict = {}
            miller_list = []

            for miller_index in self.miller_dict[key]:

                print "key", key
                print 'miller', miller_index

                criteria = {'chemsys':key, 'miller_index': miller_index}
                slab_criteria = criteria.copy()
                slab_criteria['structure_type'] = 'slab_cell'
                unit_criteria = criteria.copy()
                unit_criteria['structure_type'] = 'oriented_unit_cell'
                print slab_criteria

                slab_entry = qe.get_entries(slab_criteria,
                                            optional_data=optional_data)
                print slab_entry
                oriented_ucell_entry = qe.get_entries(unit_criteria,
                                                      optional_data=optional_data)
                print len(oriented_ucell_entry)

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