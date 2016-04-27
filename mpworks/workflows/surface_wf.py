
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
import copy
from pymongo import MongoClient
db = MongoClient().data

from mpworks.firetasks_staging.surface_tasks import RunCustodianTask, \
    VaspSlabDBInsertTask, WriteSlabVaspInputs, WriteUCVaspInputs, \
    WriteAtomVaspInputs
from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    UnconvergedErrorHandler, PotimErrorHandler, PositiveEnergyErrorHandler, \
    FrozenJobErrorHandler
# from custodian.vasp.surface_handlers import SurfaceFrozenJobErrorHandler, \
#     SurfacePositiveEnergyErrorHandler, SurfacePotimErrorHandler, \
#     SurfaceVaspErrorHandler

from pymatgen.core.surface import SlabGenerator, GetMillerIndices
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad
from matgendb import QueryEngine


class SurfaceWorkflowManager(object):

    """
        Initializes the workflow manager by taking in a list of
        formulas/mpids or a dictionary with the formula as the key referring
        to a list of miller indices.
    """

    def __init__(self, elements_and_mpids=[], indices_dict=None, ucell_dict={},
                 slab_size=10, vac_size=10, host=None, port=None, user=None,
                 password=None, symprec=0.001, angle_tolerance=5, database=None,
                 collection="surface_tasks", fail_safe=True, reset=False,
                 ucell_indices_dict={}, check_exists=True):

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
                ucell_dict ({some name or mpid: structure, ...}): A dictionary of
                    conventional unit cells with custom names as keys. Will get a indices_
                    dict using max index
                ucell_indices_dict ({some name or mpid: {"ucell": structure, "hkl_list": [(), ...]} ...}):
                    A dictionary of conventional unit cells and miller indices with custom
                    names as keys. Behave similarly to indices_dict
                host (str): For database insertion
                port (int): For database insertion
                user (str): For database insertion
                password (str): For database insertion
                symprec (float): See SpaceGroupAnalyzer in analyzer.py
                angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
                database (str): For database insertion
                collection (str): For database insertion
                fail_safe (bool): Check for slabs/bulk structures with
                    more than 200 atoms (defaults to True)
                reset (bool): Reset your launchpad (defaults to False)
                check_exists (bool): Check if slab/bulk calculation
                    has already been completed
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

        if indices_dict:
            compounds = indices_dict.keys()
        elif ucell_dict:
            compounds = ucell_dict.keys()
        elif ucell_indices_dict:
            compounds = ucell_indices_dict.keys()
        else:
            compounds = copy.copy(elements_and_mpids)


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

            if ucell_dict:

                conv_unit_cell = ucell_dict[el]
                spa = SpacegroupAnalyzer(conv_unit_cell, symprec=symprec,
                                         angle_tolerance=angle_tolerance)
                spacegroup = spa.get_spacegroup_symbol()
                polymorph_order = float("nan")
                mpid = copy.copy(el)

            elif ucell_indices_dict:
                conv_unit_cell = ucell_indices_dict[el]["ucell"]
                spa = SpacegroupAnalyzer(conv_unit_cell, symprec=symprec,
                                         angle_tolerance=angle_tolerance)
                spacegroup = spa.get_spacegroup_symbol()
                polymorph_order = float("nan")
                mpid = copy.copy(el)
                new_indices_dict[mpid] = ucell_indices_dict[mpid]["hkl_list"]
            else:
                entries = mprest.get_entries(el, inc_structure="final")
                print "# of entries for %s: " %(el), len(entries)

                # First, let's get the order of energy values of,
                # polymorphs so we can rank them by stability
                formula = entries[0].structure.composition.reduced_formula
                all_entries = mprest.get_entries(formula, inc_structure="final",
                                                 property_data=["material_id"])
                e_per_atom = [entry.energy_per_atom for entry in all_entries]
                e_per_atom, all_entries = zip(*sorted(zip(e_per_atom, all_entries)))
                mpids = [entry.data["material_id"] for entry in all_entries]

                if el[:2] != 'mp':
                    # Retrieve the ground state structure if a
                    # formula is given instead of a material ID
                    prim_unit_cell = all_entries[0].structure
                    mpid = all_entries[0].data["material_id"]
                    polymorph_order = 0
                else:
                    # If we get a material ID instead, get its polymorph rank
                    prim_unit_cell = entries[0].structure
                    mpid = el
                    polymorph_order = mpids.index(mpid)

                if indices_dict:
                    new_indices_dict[mpid] = indices_dict[mpid]

                # Get the spacegroup of the conventional unit cell
                spa = SpacegroupAnalyzer(prim_unit_cell, symprec=symprec,
                                         angle_tolerance=angle_tolerance)
                conv_unit_cell = spa.get_conventional_standard_structure()
                print conv_unit_cell
                spacegroup = mprest.get_data(mpid, prop="spacegroup")[0]["spacegroup"]["symbol"]
                print spacegroup

            # Get a dictionary of different properties for a particular material
            unit_cells_dict[mpid] = {"ucell": conv_unit_cell, "spacegroup": spacegroup,
                                     "polymorph": polymorph_order}
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
        self.surface_query_engine = QueryEngine(**vaspdbinsert_params)
        self.check_exists = check_exists

    def from_max_index(self, max_index, max_normal_search=True, max_only=False, get_bulk_e=True):

        """
            Class method to create a surface workflow with a list of unit cells
            based on the max miller index. Used in combination with list_of_elements

                Args:
                    max_index (int): The maximum miller index to create slabs from
                    max_normal_search (bool): Whether or not to orthogonalize slabs
                        and oriented unit cells along the c direction.
                    max_only (bool): Will retrieve workflows for Miller indices
                        containing the max index only
                    get_bulk_e (bool): Whether or not to get the bulk
                        calculations, obsolete if check_exist=True
        """

        # All CreateSurfaceWorkflow objects take in a dictionary
        # with a mpid key and list of indices as the item
        miller_dict = {}
        for mpid in self.unit_cells_dict.keys():
            max_miller = []
            list_of_indices = \
                GetMillerIndices(self.unit_cells_dict[mpid]['ucell'],
                                 max_index).get_symmetrically_distinct_miller_indices()

            print '# ', mpid

            # Will only return slabs whose indices
            # contain the max index if max_only = True
            if max_only:
                for hkl in list_of_indices:
                    if abs(min(hkl)) == max_index or abs(max(hkl)) == max_index:
                        max_miller.append(hkl)
                miller_dict[mpid] = max_miller
            else:
                miller_dict[mpid] = list_of_indices

        if self.check_exists:
            return self.check_existing_entries(miller_dict, max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e)

    def from_list_of_indices(self, list_of_indices, max_normal_search=True, get_bulk_e=True):

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

        if self.check_exists:
            return self.check_existing_entries(miller_dict,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e)

    def from_indices_dict(self, max_normal_search=True, get_bulk_e=True):

        """
            Class method to create a surface workflow with a dictionary with the keys
            being the formula of the unit cells we want to create slabs from which
            will refer to a list of miller indices.
            eg. indices_dict={'Fe': [[1,1,0]], 'LiFePO4': [[1,1,1], [2,2,1]]}
        """

        if self.check_exists:
            return self.check_existing_entries(self.indices_dict,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(self.indices_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e)


    def check_existing_entries(self, miller_dict, max_normal_search=True):

        # Checks if a calculation is already in the DB to avoid
        # calculations that are already finish and creates workflows
        # with structures that have not been calculated yet.

        criteria = {'state': 'successful'}

        # To keep track of which calculations
        # are done and can be skipped
        calculate_with_bulk = {}
        calculate_with_slab_only = {}
        total_calculations = 0
        total_calcs_finished = 0
        total_calcs_with_bulk = 0
        total_calcs_with_nobulk = 0

        for mpid in miller_dict.keys():
            total_calculations += len(miller_dict[mpid])
            for hkl in miller_dict[mpid]:
                criteria['structure_type'] = 'oriented_unit_cell'
                criteria['material_id'] = mpid
                criteria['miller_index'] = hkl

                # Check if the oriented unit cell
                # has already been calculated

                ucell_entries = self.surface_query_engine.get_entries(criteria, inc_structure="Final")

                if ucell_entries:
                    print '%s %s oriented unit cell already calculated, ' \
                          'now checking for existing slab' %(mpid, hkl)

                    # Check if slab calculations are complete if the
                    # oriented unit cell has already been calculated

                    criteria['structure_type'] = 'slab_cell'
                    slab_entries = self.surface_query_engine.get_entries(criteria, inc_structure="Final")

                    if slab_entries:
                        continue

                    else:
                        # No slab calculations found, insert slab calculations
                        if mpid not in calculate_with_slab_only.keys():
                            calculate_with_slab_only[mpid] = []
                        print '%s %s slab cell not in DB, ' \
                              'will insert calculation into WF' %(mpid, hkl)
                        calculate_with_slab_only[mpid].append(hkl)
                        total_calcs_with_nobulk += 1
                else:

                    # Insert complete calculation for oriented ucell and
                    # slabs if no oriented ucell calculation has been found
                    if mpid not in calculate_with_bulk.keys():
                        calculate_with_bulk[mpid] = []
                    print '%s %s oriented unit  cell not in DB, ' \
                          'will insert calculation into WF' %(mpid, hkl)
                    calculate_with_bulk[mpid].append(hkl)
                    total_calcs_with_bulk +=1

        # Get the parameters for CreateSurfaceWorkflow. Will create two separate WFs,
        # one that calculates slabs only and one that calculates slabs and oriented ucells

        wf_kwargs = {'unit_cells_dict': self.unit_cells_dict,
                     'vaspdbinsert_params': self.vaspdbinsert_params,
                     'ssize': self.ssize, 'vsize': self.vsize,
                     'max_normal_search': max_normal_search,
                     'fail_safe': self.fail_safe, 'reset': self.reset}

        with_bulk = CreateSurfaceWorkflow(calculate_with_bulk,
                                          get_bulk_e=True, **wf_kwargs)

        with_slab_only = CreateSurfaceWorkflow(calculate_with_slab_only,
                                               get_bulk_e=False, **wf_kwargs)

        print "total number of Indices: ", total_calculations
        print
        print "total number of calculations with no bulk: ", total_calcs_with_nobulk, calculate_with_slab_only
        print
        print "total number of calculations with bulk: ", total_calcs_with_bulk, calculate_with_bulk
        print
        print "total number of calculations already finished: ", total_calcs_finished
        print

        return [with_bulk, with_slab_only]


class CreateSurfaceWorkflow(object):

    """
        A class for creating surface workflows and creating a dicionary of all
        calculated surface energies and wulff shape objects. Don't actually
        create an object of this class manually, instead use
        SurfaceWorkflowManager to create an object of this class.
    """

    def __init__(self, miller_dict, unit_cells_dict, vaspdbinsert_params,
                 ssize, vsize, max_normal_search=True,
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

        surface_query_engine = QueryEngine(**vaspdbinsert_params)

        self.surface_query_engine = surface_query_engine
        self.miller_dict = miller_dict
        self.unit_cells_dict = unit_cells_dict
        self.vaspdbinsert_params = vaspdbinsert_params
        self.max_normal_search = max_normal_search
        self.ssize = ssize
        self.vsize = vsize
        self.reset = reset
        self.fail_safe = fail_safe
        self.get_bulk_e = get_bulk_e

    def launch_workflow(self, launchpad_dir="", k_product=50, job=None, gpu=False,
                        user_incar_settings=None, potcar_functional='PBE', oxides=False,
                        additional_handlers=[], scratch_dir=None):

        """
            Creates a list of Fireworks. Each Firework represents calculations
            that will be done on a slab system of a compound in a specific
            orientation. Each Firework contains a oriented unit cell relaxation job
            and a WriteSlabVaspInputs which creates Firework and additionals depending
            on whether or not Termination=True. VASP outputs from all slab and
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

        handlers = [NonConvergingErrorHandler(change_algo=True),
                    UnconvergedErrorHandler(),
                    PotimErrorHandler(),
                    PositiveEnergyErrorHandler(),
                    VaspErrorHandler(),
                    FrozenJobErrorHandler(output_filename="OSZICAR", timeout=3600)]#,
                    # If none of the usual custodian handlers work, use the
                    # altered surface specific handlers as a last resort
                    # SurfacePositiveEnergyErrorHandler(),
                    # SurfacePotimErrorHandler(),
                    # SurfaceVaspErrorHandler(),
                    # SurfaceFrozenJobErrorHandler(output_filename="OSZICAR")]

        if additional_handlers:
            handlers.extend(additional_handlers)

        scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir

        cust_params = {"scratch_dir": os.path.join(scratch_dir, os.environ["USER"]),
                       "jobs": job.double_relaxation_run(job.vasp_cmd,
                                                         auto_npar=False),
                       "handlers": handlers,
                       "max_errors": 10}  # will return a list of jobs
                                           # instead of just being one job

        fws = []
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
                    print "UCELL EXCEEDED 199 ATOMS!!!"
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
                                                    folder=folderbulk, cwd=cwd, gpu=gpu,
                                                    user_incar_settings=user_incar_settings,
                                                    potcar_functional=potcar_functional,
                                                    k_product=k_product, oxides=oxides),
                                 RunCustodianTask(dir=folderbulk, cwd=cwd,
                                                  custodian_params=cust_params),
                                 VaspSlabDBInsertTask(struct_type="oriented_unit_cell",
                                                      loc=folderbulk, cwd=cwd,
                                                      miller_index=miller_index, mpid=mpid,
                                                      conventional_unit_cell=self.unit_cells_dict[mpid]["ucell"],
                                                      conventional_spacegroup=self.unit_cells_dict[mpid]['spacegroup'],
                                                      polymorph=self.unit_cells_dict[mpid]["polymorph"],
                                                      vaspdbinsert_parameters=self.vaspdbinsert_params)])

                tasks.extend([WriteSlabVaspInputs(folder=folderbulk, cwd=cwd,
                                                  user_incar_settings=user_incar_settings,
                                                  custodian_params=cust_params,
                                                  vaspdbinsert_parameters=
                                                  self.vaspdbinsert_params,
                                                  potcar_functional=potcar_functional,
                                                  k_product=k_product, gpu=gpu,
                                                  miller_index=miller_index,
                                                  min_slab_size=self.ssize,
                                                  conventional_unit_cell=self.unit_cells_dict[mpid]["ucell"],
                                                  min_vacuum_size=self.vsize, mpid=mpid,
                                                  conventional_spacegroup=self.unit_cells_dict[mpid]['spacegroup'],
                                                  polymorph=self.unit_cells_dict[mpid]["polymorph"],
                                                  oxides=oxides)])

                fw = Firework(tasks, name=folderbulk)

                fws.append(fw)
                print self.unit_cells_dict[mpid]['spacegroup']
        wf = Workflow(fws, name='Surface Calculations')
        launchpad.add_wf(wf)


def atomic_energy_workflow(host=None, port=None, user=None, password=None, database=None,
                           collection="Surface_Collection", latt_a=16, kpoints=1, job=None,
                           scratch_dir=None, additional_handlers=[], launchpad_dir="",
                           elements=[], user_incar_settings={}):

    """
    A simple workflow for calculating a single isolated atom in a box
    and inserting it into the surface database. Values are useful for
    things like getting cohesive energies in bulk structures. Kind of
    a one trick pony since there's only a handful of elements, but whatever.

        Args:
    """

    vaspdbinsert_params = {'host': host,
                           'port': port, 'user': user,
                           'password': password,
                           'database': database,
                           'collection': collection}
    if not elements:
        elements = QueryEngine(**vaspdbinsert_params).collection.distinct("pretty_formula")

    launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                 launchpad_dir,
                                                 "my_launchpad.yaml"))
    launchpad.reset('', require_password=False)

    if not job:
        job = VaspJob(["mpirun", "-n", "64", "vasp"],
                      auto_npar=False, copy_magmom=True)

    handlers = [VaspErrorHandler(),
                NonConvergingErrorHandler(nionic_steps=4, change_algo=True),
                UnconvergedErrorHandler(),
                PotimErrorHandler(),
                PositiveEnergyErrorHandler(),
                FrozenJobErrorHandler(output_filename="OSZICAR", timeout=7200)]
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
                   "max_errors": 10,
                   "skip_over_errors": True}  # will return a list of jobs instead of just being one job

    fws = []
    for el in elements:
        folder_atom = '/%s_isolated_atom_%s_k%s' %(el, latt_a, kpoints)
        cwd = os.getcwd()

        tasks = [WriteAtomVaspInputs(atom=el, folder=folder_atom, cwd=cwd,
                                     latt_a=latt_a, kpoints=kpoints,
                                     user_incar_settings=user_incar_settings),
                 RunCustodianTask(dir=folder_atom, cwd=cwd,
                                              **cust_params),
                 VaspSlabDBInsertTask(struct_type="isolated_atom",
                                      loc=folder_atom, cwd=cwd,
                                      miller_index=None, mpid=None,
                                      conventional_spacegroup=None,
                                      conventional_unit_cell=None,
                                      isolated_atom=el,
                                      polymorph=None,
                                      vaspdbinsert_parameters=vaspdbinsert_params)]

        fws.append(Firework(tasks, name=folder_atom))

    wf = Workflow(fws, name='Surface Calculations')
    launchpad.add_wf(wf)
