
## for Surface Energy Calculation
from __future__ import division, unicode_literals
__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"


import os

from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspSlabDBInsertTask, WriteSlabVaspInputs, WriteUCVaspInputs
from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    UnconvergedErrorHandler
from pymatgen.core.surface import generate_all_slabs, SlabGenerator, \
    get_symmetrically_distinct_miller_indices
from pymatgen.core.surface import SlabGenerator, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester
from pymatgen.io.vaspio.vasp_output import Outcar

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from matgendb import QueryEngine

import socket
hostname = socket.gethostname()
if hostname[:3] != 'cvr':
    from pymatgen.analysis.wulff_dual import wulff_3d

class SurfaceWorkflowManager(object):

    """
        Initializes the workflow manager by taking in a list of compounds in their
        compositional formula or a dictionary with the formula as the key referring
        to a list of miller indices.

    """

    def __init__(self, api_key, list_of_elements=[], indices_dict=None, slab_size=10, vac_size=10,
                 host=None, port=None, user=None, password=None,
                 symprec=0.001, angle_tolerance=5, database=None, collection="Surface_Collection", reset=False):

        """
            Args:
                api_key (str): A String API key for accessing the MaterialsProject
                list_of_elements ([str, ...]): A list of compounds or elements to create
                    slabs from. Must be a string that can be searched for with MPRester.
                    Either list_of_elements or indices_dict has to be entered in.
                indices_dict ({element(str): [[h,k,l], ...]}): A dictionary of
                    miller indices corresponding to the composition formula
                    (key) to transform into a list of slabs. Either list_of_elements
                    or indices_dict has to be entered in.
                host (str): For database insertion
                port (int): For database insertion
                user (str): For database insertion
                password (str): For database insertion
                symprec (float): See SpaceGroupAnalyzer in analyzer.py
                angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
                database (str): For database insertion
        """

        unit_cells_dict = {}
        vaspdbinsert_params = {'host': host,
                               'port': port, 'user': user,
                               'password': password,
                               'database': database,
                               'collection': collection}

        elements = [key for key in indices_dict.keys()] \
            if indices_dict else list_of_elements

        # For loop will eneumerate through all the compositional
        # formulas in list_of_elements or indices_dict to get a
        # list of relaxed conventional unit cells froom MP. These
        # will be used to generate all oriented unit cells and slabs.

        for el in elements:

            """
            element: str, element name of Metal
            miller_index: hkl, e.g. [1, 1, 0]
            api_key: to get access to MP DB
            """

            # This initializes the REST adaptor. Put your own API key in.
            mprest = MPRester(api_key)
            #Returns a list of MPIDs with the compositional formular, the
            # first MPID IS NOT the lowest energy per atom
            entries = mprest.get_entries(el, inc_structure="final")

            e_per_atom = [entry.energy_per_atom for entry in entries]
            for entry in entries:
                if min(e_per_atom) == entry.energy_per_atom:
                    prim_unit_cell = entry.structure

            spa = SpacegroupAnalyzer(prim_unit_cell, symprec=symprec,
                                     angle_tolerance=angle_tolerance)
            conv_unit_cell = spa.get_conventional_standard_structure()
            print conv_unit_cell
            unit_cells_dict[el] = [conv_unit_cell, min(e_per_atom)]
            print el


        self.api_key = api_key
        self.vaspdbinsert_params = vaspdbinsert_params
        self.symprec = symprec
        self.angle_tolerance = angle_tolerance
        self.unit_cells_dict = unit_cells_dict
        self.indices_dict = indices_dict
        self.elements = elements
        self.ssize = slab_size
        self.vsize = vac_size
        self.reset = reset


    def from_max_index(self, max_index, max_normal_search=True, terminations=False):

        """
            Class method to create a surface workflow with a list of unit cells
            based on the max miller index. Used in combination with list_of_elements

                Args:
                    max_index (int): The maximum miller index to create slabs from
                    max_normal_search (bool): Whether or not to orthogonalize slabs
                        and oriented unit cells along the c direction.
                    terminations (bool): Whether or not to consider the different
                        possible terminations in a slab. If set to false, only one
                        slab is calculated per miller index with the shift value
                        set to 0.
        """

        max_norm=max_index if max_normal_search else None
        miller_dict = {}
        for el in self.elements:
            # generate_all_slabs() is very slow, especially for Mn
            list_of_indices = \
                get_symmetrically_distinct_miller_indices(self.unit_cells_dict[el][0],
                                                          max_index)

            print 'surface ', el

            print '# ', el

            miller_dict[el] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params, ssize=self.ssize, vsize=self.vsize,
                                     max_normal_search=max_normal_search, terminations=terminations, reset=self.reset)


    def from_list_of_indices(self, list_of_indices, max_normal_search=True,
                             terminations=False):

        """
            Class method to create a surface workflow with a
            list of unit cells based on a list of miller indices.

                Args:
                    list_of_indices (list of indices): eg. [[h,k,l], [h,k,l], ...etc]
                    A list of miller indices to generate slabs from.
                    Used in combination with list_of_elements.
        """

        miller_dict = {}
        for el in self.elements:
            miller_dict[el] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params, ssize=self.ssize, vsize=self.vsize,
                                     max_normal_search=max_normal_search, terminations=terminations, reset=self.reset)


    def from_indices_dict(self, max_normal_search=True, terminations=False):

        """
            Class method to create a surface workflow with a dictionary with the keys
            being the formula of the unit cells we want to create slabs from which
            will refer to a list of miller indices.
            eg. indices_dict={'Fe': [[1,1,0]], 'LiFePO4': [[1,1,1], [2,2,1]]}
        """

        return CreateSurfaceWorkflow(self.indices_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params, ssize=self.ssize, vsize=self.vsize,
                                     max_normal_search=max_normal_search, terminations=terminations, reset=self.reset)


class CreateSurfaceWorkflow(object):

    """
        A class for creating surface workflows and creating a dicionary of all
        calculated surface energies and wulff shape objects. Don't actually
        create an object of this class manually, instead use
        SurfaceWorkflowManager to create an object of this class.
    """

    def __init__(self, miller_dict, unit_cells_dict, vaspdbinsert_params, ssize, vsize,
                 terminations=False, max_normal_search=True, reset=False):

        """
            Args:
                miller_dict (ditionary): Each class method from SurfaceWorkflowManager
                    will create a dictionary similar to indices_dict (see previous doc).
                unit_cells_dict (dictionary): A dictionary of unit cells with the
                    formula of the unit cell being the key reffering to a Structure
                    object taken from MP, eg.
                    unit_cells_dict={'Cr': <structure object>, 'LiCoO2': <structure object>}
                vaspdbinsert_params (dictionary): A kwargs used for the VaspSlabDBInsertTask
                    containing information pertaining to the database that the vasp
                    outputs will be inserted into,
                    ie vaspdbinsert_params = {'host': host,'port': port, 'user': user,
                                              'password': password, 'database': database}
        """

        self.miller_dict = miller_dict
        self.unit_cells_dict = unit_cells_dict
        self.vaspdbinsert_params = vaspdbinsert_params
        self.max_normal_search = max_normal_search
        self.terminations = terminations
        self.ssize = ssize
        self.vsize = vsize
        self.reset = reset


    def launch_workflow(self, launchpad_dir="",
                        k_product=50, cwd=os.getcwd(),
                        job=VaspJob(["mpirun", "-n", "16", "vasp"]),
                        user_incar_settings=None, potcar_functional='PBE', get_bulk_e=True):

        """
            Creates a list of Fireworks. Each Firework represents calculations
            that will be done on a slab system of a compound in a specific
            orientation. Each Firework contains a oriented unit cell relaxation job
            and a WriteSlabVaspInputs which creates os. Firework(s) depending
            on whether or not Termination=True. Vasp outputs from all slab and
            oriented unit cell calculations will then be inserted into a database.

            Args:
                launchpad_dir (str path): The path to my_launchpad.yaml. Defaults to
                    the current working directory containing your runs
                k_product: kpts[0][0]*a. Decide k density without
                    kpoint0, default to 50
                cwd: (str path): The curent working directory. Location of where you
                    want your vasp outputs to be.
                job (VaspJob): The command (cmd) entered into VaspJob object. Default
                    is specifically set for running vasp jobs on Carver at NERSC
                    (use aprun for Hopper or Edison).
                user_incar_settings(dict): A dict specifying additional incar
                    settings, default to None (ediff_per_atom=False)
                potcar_functional (str): default to PBE
        """

        launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                     launchpad_dir,
                                                     "my_launchpad.yaml"))
        if self.reset:
            launchpad.reset('', require_password=False)

        # Scratch directory reffered to by custodian.
        # May be different on non-Nersc systems.
        cust_params = {"custodian_params":
                           {"scratch_dir":
                                os.path.join("/global/scratch2/sd/",
                                             os.environ["USER"])},
                       "jobs": job.double_relaxation_run(job.vasp_cmd)} # will return a list of jobs
                                                                        # instead of just being on job

        fws=[]
        for key in self.miller_dict.keys():
            # Enumerate through all compounds in the dictionary,
            # the key is the compositional formula of the compound
            print key
            for miller_index in self.miller_dict[key]:
                # Enumerates through all miller indices we
                # want to create slabs of that compound from

                print str(miller_index)

                max_norm = max(miller_index) if self.max_normal_search else None
                # Whether or not we want to use the
                # max_normal_search algorithm from surface.py
                print 'true or false max norm is ', max_norm, self.max_normal_search

                slab = SlabGenerator(self.unit_cells_dict[key][0], miller_index,
                                     self.ssize, self.vsize, max_normal_search=max_norm)
                oriented_uc = slab.oriented_unit_cell
                # This method only creates the oriented unit cell, the
                # slabs are created in the WriteSlabVaspInputs task.
                # WriteSlabVaspInputs will create the slabs from
                # the contcar of the oriented unit cell calculation
                handler = []
                tasks = []

                folderbulk = '/%s_%s_k%s_s%sv%s_%s%s%s' %(oriented_uc.composition.reduced_formula,
                                                   'bulk', k_product, self.ssize, self.vsize,
                                                   str(miller_index[0]),
                                                   str(miller_index[1]),
                                                   str(miller_index[2]))
                if get_bulk_e:
                    tasks.extend([WriteUCVaspInputs(oriented_ucell=oriented_uc,
                                               folder=cwd+folderbulk,
                                               user_incar_settings=user_incar_settings,
                                               potcar_functional=potcar_functional,
                                               k_product=k_product),
                                 RunCustodianTask(dir=cwd+folderbulk,
                                                  handlers=[VaspErrorHandler(),
                                                            NonConvergingErrorHandler(),
                                                            UnconvergedErrorHandler()],
                                                  **cust_params),
                                 VaspSlabDBInsertTask(struct_type="oriented_unit_cell",
                                                      loc=cwd+folderbulk,
                                                      miller_index=miller_index,
                                                      **self.vaspdbinsert_params)])

                    # Slab will inherit average final magnetic moment
                    # of the bulk from outcar, will have to generalize
                    # this for systems with different elements later
                    # element = oriented_uc.species[0]
                    # out = Outcar(cwd+folderbulk)
                    # out_mag = out.magnetization
                    # tot_mag = [mag['tot'] for mag in out_mag]
                    # magmom = np.mean(tot_mag)
                    # user_incar_settings['MAGMOM'] = {element: magmom}

                tasks.append(WriteSlabVaspInputs(folder=cwd+folderbulk,
                                                 user_incar_settings=user_incar_settings,
                                                 terminations=self.terminations,
                                                 custodian_params=cust_params,
                                                 vaspdbinsert_parameters=
                                                 self.vaspdbinsert_params,
                                                 potcar_functional=potcar_functional,
                                                 k_product=k_product,
                                                 miller_index=miller_index,
                                                 min_slab_size=self.ssize,
                                                 min_vacuum_size=self.vsize,
                                                 get_bulk_e=get_bulk_e,
                                                 ucell=self.unit_cells_dict[key][0]))

                fw = Firework(tasks, name=folderbulk)

                fws.append(fw)
        wf = Workflow(fws, name='Surface Calculations')
        launchpad.add_wf(wf)


    def get_energy_and_wulff(self, bulk_e_from_mp=False):

        """
            This method queries a database to calculate
            all surface energies as well as wulff shapes
            for all calculations ran by the workflow
            created by the same object being used
        """

        qe = QueryEngine(**self.vaspdbinsert_params)

        # Data needed from DB to perform calculations
        optional_data = ["chemsys", "surface_area", "nsites",
                         "structure_type", "miller_index",
                         "shift", "vac_size", "slab_size", "state"]

        to_Jperm2 = 16.0217656
        wulffshapes = {}
        surface_energies = {}
        print 'miller dictionary is ', self.miller_dict
        for el in self.miller_dict.keys():
            # Each loop generates and wulff shape object and puts
            # it in a wulffshapes dictionary where the key is the
            # compositional formula of the material used to obtain
            # the surface energies to generate the shape

            e_surf_list = []
            se_dict = {}
            miller_list = []
            success = True

            print 'current key is ', el

            for miller_index in self.miller_dict[el]:
                # Each loop generates a surface energy value
                # corresponding to a material and a miller index.
                # Append to se_dict where the key is the miller index

                print "key", el
                print self.miller_dict[el]
                print 'miller', miller_index

                # Get entry of oriented unit cell calculation
                # and its corresponding slab calculation
                criteria = {'chemsys':el, 'miller_index': miller_index}
                slab_criteria = criteria.copy()
                slab_criteria['structure_type'] = 'slab_cell'
                unit_criteria = criteria.copy()
                unit_criteria['structure_type'] = 'oriented_unit_cell'
                # print slab_criteria

                slab_entry = qe.get_entries(slab_criteria,
                                            optional_data=optional_data)
                print len(slab_entry)
                # print '# of unit entries', len(oriented_ucell_entry)
                oriented_ucell_entry = qe.get_entries(unit_criteria, optional_data=optional_data)
                print oriented_ucell_entry
                if oriented_ucell_entry==[] or slab_entry==[]:
                    "%s Firework was unsuccessful" \
                    %(el)
                    success=False
                    continue

                if oriented_ucell_entry[0].data['state'] != "successful" or \
                                slab_entry[0].data['state'] != "successful":
                    "%s Firework was unsuccessful" \
                    %(el)
                    success=False
                    continue
                # print oriented_ucell_entry
                print

                # Calculate SE of each termination
                se_term = {}
                min_e = []
                for slab in slab_entry:
                    slabE = slab.uncorrected_energy
                    if bulk_e_from_mp:
                        bulkE = self.unit_cells_dict[el][1]*slab.data['nsites']
                    else:
                        bulkE = oriented_ucell_entry[0].energy_per_atom*\
                                slab.data['nsites']
                    area = slab.data['surface_area']
                    se_term[str(slab.data['shift'])] = \
                        ((slabE-bulkE)/(2*area))*to_Jperm2

                # Get the lowest SE of the various
                # terminations to build the wulff shape from
                min_e = [se_term[shift] for shift in se_term.keys()]
                print min_e
                e_surf_list.append(min(min_e))
                se_dict[str(miller_index)] = se_term
                miller_list.append(miller_index)

            # Create the wulff shape with the lowest surface
            # energies in slabs with multiple terminations
            if success:
                wulffshapes[el] = wulff_3d(self.unit_cells_dict[el][0],
                                           miller_list, e_surf_list)
            else:
                print "Slab calculation set incomplete, wulff shape cannot be generated"
            surface_energies[el] = se_dict

        # Returns dictionary of wulff
        # shape objects and surface energy
        # eg. wulffshapes={'ZnO': <wulffshape object>, ...etc}
        # surface_energies={'ZnO': {(1,1,0): {0.3: 3.532, etc..}, etc ...}, etc ...}
        return wulffshapes, surface_energies

# class SingleTaskRuns(object):
#
#     def __init__(self, host, port, user, password, database, collection, launchpad_dir):
#
#         vaspdbinsert_params = {'host': host,
#                                'port': port, 'user': user,
#                                'password': password,
#                                'database': database,
#                                'collection': collection}
#
#         launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
#                                                      launchpad_dir,
#                                                      "my_launchpad.yaml"))
#
#         self.db_parameters = vaspdbinsert_params
#         self.launchpad = launchpad
#
#     def single_runcustodiantask(self, jobs, handlers=[], dir=os.getcwd()):
#
#         cust_params = {"custodian_params":
#                            {"scratch_dir":
#                                 os.path.join("/global/scratch2/sd/",
#                                              os.environ["USER"])},
#                        "jobs": jobs}
#
#         fw = Firework([RunCustodianTask(dir=dir,
#                                         handlers=[VaspErrorHandler()],
#                                         **cust_params)])
#
#         fws = [fw]
#         wf = Workflow(fws, name=self.db_parameters['collection'])
#         self.launchpad.add_wf(wf)
#
#
#     def single_vaspslabdbinserttask(self, miller_index, struct_type):
#
#         fw = Firework([VaspSlabDBInsertTask(struct_type=struct_type,
#                                             loc=dir,
#                                             miller_index=miller_index,
#                                             **self.db_parameters)])
#
#         fws = [fw]
#         wf = Workflow(fws, name=self.db_parameters['collection'])
#         self.launchpad.add_wf(wf)
