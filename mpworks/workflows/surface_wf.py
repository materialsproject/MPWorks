
## for Surface Energy Calculation
from __future__ import division, unicode_literals

"""
#TODO: Write module doc.
       Clean up
"""

__author__ = "Richard Tran"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/24/15"


import os
import uuid

from mpworks.firetasks.surface_tasks import RunCustodianTask, \
    VaspSlabDBInsertTask, WriteSlabVaspInputs, WriteUCVaspInputs, \
    MoveDirectoryTask
from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    UnconvergedErrorHandler, PotimErrorHandler, PositiveEnergyErrorHandler, \
    FrozenJobErrorHandler
from pymatgen.core.surface import SlabGenerator, \
    get_symmetrically_distinct_miller_indices
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester
from pymatgen.io.vasp.outputs import Outcar

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from matgendb import QueryEngine


class SurfaceWorkflowManager(object):

    """
        Initializes the workflow manager by taking in a list of compounds in their
        compositional formula or a dictionary with the formula as the key referring
        to a list of miller indices.
    """

    def __init__(self, elements_and_mpids=[], indices_dict=None,
                 slab_size=10, vac_size=10, host=None, port=None, user=None,
                 password=None, symprec=0.001, angle_tolerance=5, database=None,
                 collection="Surface_Collection", fail_safe=True, reset=False):

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

        # This initializes the REST adaptor. Put your own API key in.

        if "MAPI_KEY" not in os.environ:
            apikey = raw_input('Enter your api key (str): ')
        else:
            apikey = os.environ["MAPI_KEY"]

        mprest = MPRester(apikey)

        vaspdbinsert_params = {'host': host,
                               'port': port, 'user': user,
                               'password': password,
                               'database': database,
                               'collection': collection}

        compounds = [key for key in indices_dict.keys()] \
            if indices_dict else elements_and_mpids

        # For loop will enumerate through all the compositional
        # formulas in list_of_elements or indices_dict to get a
        # list of relaxed conventional unit cells from MP. These
        # will be used to generate all oriented unit cells and slabs.

        new_indices_dict = {}
        for el in compounds:

            """
            element: str, element name of Metal
            miller_index: hkl, e.g. [1, 1, 0]
            api_key: to get access to MP DB
            """

            #Returns a list of MPIDs with the compositional formula, the
            # first MPID IS NOT the lowest energy per atom
            entries = mprest.get_entries(el, inc_structure="final")

            if el[:2] != 'mp':
                e_per_atom = [entry.energy_per_atom for entry in entries]
                for i, entry in enumerate(entries):
                    if min(e_per_atom) == entry.energy_per_atom:
                        prim_unit_cell = entry.structure
                        mpid = mprest.get_data(el, prop='material_id')[i]['material_id']
                        if indices_dict:
                            new_indices_dict[mpid] = indices_dict[el]
            else:
                prim_unit_cell = entries[0].structure
                mpid = el
                if indices_dict:
                    new_indices_dict[mpid] = indices_dict[mpid]

            spa = SpacegroupAnalyzer(prim_unit_cell, symprec=symprec,
                                     angle_tolerance=angle_tolerance)
            conv_unit_cell = spa.get_conventional_standard_structure()
            print conv_unit_cell
            spacegroup = mprest.get_data(mpid, prop='spacegroup')[0]['spacegroup']['symbol']
            print spacegroup
            unit_cells_dict[mpid] = {'ucell': conv_unit_cell, 'spacegroup': spacegroup}
            print el


        self.apikey = apikey
        self.vaspdbinsert_params = vaspdbinsert_params
        self.symprec = symprec
        self.angle_tolerance = angle_tolerance
        self.unit_cells_dict = unit_cells_dict
        self.indices_dict = new_indices_dict
        self.ssize = slab_size
        self.vsize = vac_size
        self.reset = reset
        self.fail_safe = fail_safe


    def from_max_index(self, max_index, max_normal_search=True,
                       terminations=False, get_bulk_e=True, max_only=False):

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

        miller_dict = {}
        for mpid in self.unit_cells_dict.keys():
            max_miller = []
            list_of_indices = \
                get_symmetrically_distinct_miller_indices(self.unit_cells_dict[mpid]['ucell'],
                                                          max_index)

            print 'surface ', mpid

            print '# ', mpid

            if max_only:
                for hkl in list_of_indices:
                    if abs(min(hkl)) == max_index or abs(max(hkl)) == max_index:
                        max_miller.append(hkl)
                miller_dict[mpid] = max_miller
            else:
                miller_dict[mpid] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params, ssize=self.ssize,
                                     vsize=self.vsize,
                                     max_normal_search=max_normal_search,
                                     terminations=terminations,
                                     fail_safe=self.fail_safe, reset=self.reset,
                                     get_bulk_e=get_bulk_e)


    def from_list_of_indices(self, list_of_indices, max_normal_search=True,
                             terminations=False, get_bulk_e = True):

        """
            Class method to create a surface workflow with a
            list of unit cells based on a list of miller indices.

                Args:
                    list_of_indices (list of indices): eg. [[h,k,l], [h,k,l], ...etc]
                    A list of miller indices to generate slabs from.
                    Used in combination with list_of_elements.
        """

        miller_dict = {}
        for mpid in self.unit_cells_dict.keys():
            miller_dict[mpid] = list_of_indices

        return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params,
                                     ssize=self.ssize, vsize=self.vsize,
                                     max_normal_search=max_normal_search,
                                     terminations=terminations,
                                     fail_safe=self.fail_safe, reset=self.reset,
                                     get_bulk_e=get_bulk_e)


    def from_indices_dict(self, max_normal_search=True, terminations=False, get_bulk_e=True):

        """
            Class method to create a surface workflow with a dictionary with the keys
            being the formula of the unit cells we want to create slabs from which
            will refer to a list of miller indices.
            eg. indices_dict={'Fe': [[1,1,0]], 'LiFePO4': [[1,1,1], [2,2,1]]}
        """

        return CreateSurfaceWorkflow(self.indices_dict, self.unit_cells_dict,
                                     self.vaspdbinsert_params,
                                     ssize=self.ssize, vsize=self.vsize,
                                     max_normal_search=max_normal_search,
                                     terminations=terminations,
                                     fail_safe=self.fail_safe, reset=self.reset,
                                     get_bulk_e=get_bulk_e)


class CreateSurfaceWorkflow(object):

    """
        A class for creating surface workflows and creating a dicionary of all
        calculated surface energies and wulff shape objects. Don't actually
        create an object of this class manually, instead use
        SurfaceWorkflowManager to create an object of this class.
    """

    def __init__(self, miller_dict, unit_cells_dict, vaspdbinsert_params,
                 ssize, vsize, terminations=False, max_normal_search=True,
                 fail_safe=True, reset=False, get_bulk_e=True):

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
        self.fail_safe = fail_safe
        self.get_bulk_e = get_bulk_e


    def launch_workflow(self, launchpad_dir="", k_product=50, job=None,
                        user_incar_settings=None, potcar_functional='PBE',
                        additional_handlers=[], scratch_dir=None, final_directory="."):

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

        if not job:
            job = VaspJob(["mpirun", "-n", "64", "vasp"],
                          auto_npar=False, copy_magmom=True)

        handlers = [VaspErrorHandler(),
                    NonConvergingErrorHandler(),
                    UnconvergedErrorHandler(),
                    PotimErrorHandler(),
                    PositiveEnergyErrorHandler(),
                    FrozenJobErrorHandler(timeout=3600)]
        if additional_handlers:
            handlers.extend(additional_handlers)

        scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir

        cust_params = {"custodian_params":
                           {"scratch_dir":
                                os.path.join(scratch_dir,
                                             os.environ["USER"])},
                       "jobs": job.double_relaxation_run(job.vasp_cmd,
                                                         auto_npar=False),
                       "handlers": handlers,
                       "max_errors": 100}  # will return a list of jobs
                                           # instead of just being one job

        fws=[]
        for mpid in self.miller_dict.keys():
            # Enumerate through all compounds in the dictionary,
            # the key is the compositional formula of the compound
            print mpid
            for miller_index in self.miller_dict[mpid]:
                # Enumerates through all miller indices we
                # want to create slabs of that compound from

                print str(miller_index)

                max_norm = max(miller_index) if self.max_normal_search else None
                # Whether or not we want to use the
                # max_normal_search algorithm from surface.py
                print 'true or false max norm is ', max_norm, self.max_normal_search

                slab = SlabGenerator(self.unit_cells_dict[mpid]['ucell'], miller_index,
                                     self.ssize, self.vsize, max_normal_search=max_norm,
                                     primitive=False)
                oriented_uc = slab.oriented_unit_cell

                if self.fail_safe and len(oriented_uc)> 199:
                    break
                # This method only creates the oriented unit cell, the
                # slabs are created in the WriteSlabVaspInputs task.
                # WriteSlabVaspInputs will create the slabs from
                # the contcar of the oriented unit cell calculation
                tasks = []

                folderbulk = '/%s_%s_%s_k%s_s%sv%s_%s%s%s' %(oriented_uc.composition.reduced_formula,
                                                             mpid,'bulk', k_product, self.ssize,
                                                             self.vsize, str(miller_index[0]),
                                                             str(miller_index[1]), str(miller_index[2]))
                cwd = os.getcwd()
                if self.get_bulk_e:
                    tasks.extend([WriteUCVaspInputs(oriented_ucell=oriented_uc,
                                               folder=folderbulk, cwd=cwd,
                                               user_incar_settings=user_incar_settings,
                                               potcar_functional=potcar_functional,
                                               k_product=k_product),
                                 RunCustodianTask(dir=folderbulk, cwd=cwd,
                                                  **cust_params),
                                 VaspSlabDBInsertTask(struct_type="oriented_unit_cell",
                                                      loc=folderbulk, cwd=cwd,
                                                      miller_index=miller_index, mpid=mpid,
                                                      conventional_spacegroup=self.unit_cells_dict[mpid]['spacegroup'],
                                                      **self.vaspdbinsert_params)])

                tasks.extend([WriteSlabVaspInputs(folder=folderbulk, cwd=cwd,
                                                  user_incar_settings=user_incar_settings,
                                                  terminations=self.terminations,
                                                  custodian_params=cust_params,
                                                  vaspdbinsert_parameters=
                                                  self.vaspdbinsert_params,
                                                  potcar_functional=potcar_functional,
                                                  k_product=k_product,
                                                  miller_index=miller_index,
                                                  min_slab_size=self.ssize,
                                                  min_vacuum_size=self.vsize, mpid=mpid,
                                                  conventional_spacegroup=self.unit_cells_dict[mpid]['spacegroup']),
                              MoveDirectoryTask(cwd=cwd, formula=oriented_uc.composition.reduced_formula,
                                                miller_index=miller_index, mpid=mpid,
                                                final_directory=final_directory)])

                fw = Firework(tasks, name=folderbulk)

                fws.append(fw)
                print self.unit_cells_dict[mpid]['spacegroup']
        wf = Workflow(fws, name='Surface Calculations')
        launchpad.add_wf(wf)
