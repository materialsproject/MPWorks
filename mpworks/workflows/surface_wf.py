
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


try:
    # New Py>=3.5 import
    from math import gcd
except ImportError:
    # Deprecated import from Py3.5 onwards.
    from fractions import gcd

import os
import copy

from pymongo import MongoClient

db = MongoClient().data

from mpworks.firetasks_staging.surface_tasks import RunCustodianTask, \
    VaspSlabDBInsertTask, WriteUCVaspInputs, \
    WriteAtomVaspInputs, GenerateFwsTask, GetMillerIndices, get_conventional_ucell

from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    UnconvergedErrorHandler, PotimErrorHandler, PositiveEnergyErrorHandler, \
    FrozenJobErrorHandler

from pymatgen.core.surface import \
    get_symmetrically_distinct_miller_indices, generate_all_slabs
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester
from pymatgen.analysis.structure_analyzer import VoronoiConnectivity
from pymatgen import Element

from matgendb import QueryEngine

from fireworks.core.firework import Firework, Workflow
from fireworks.core.launchpad import LaunchPad


def get_all_wfs(job, scratch_dir, vaspdbinsert_params, limit_atoms=10,
                collection_only=False, less_than_ehull=0.001, specific=[],
                avoid=["mp-37", "mp-85", "mp-67", "mp-160", "mp-165",
                       "mp-568286", "mp-48", "mp-568348", "mp-96",
                       "mp-142", "mp-11", "mp-570481", "mp-35"],
                collection="surface_tasks", user_incar_settings={"EDIFF": 1e-04},
                gpu=False, launchpad_dir=""):

    # Makes running calculations on all solid systems less of a hassle every time

    # This initializes the REST adaptor. Put your own API key in.
    if "MAPI_KEY" not in os.environ:
        apikey = input('Enter your api key (str): ')
    else:
        apikey = os.environ["MAPI_KEY"]

    mprester = MPRester(apikey)

    cubic, non_cubic = [], []

    if collection_only:

        conn = MongoClient(host=vaspdbinsert_params["host"],
                           port=vaspdbinsert_params["port"])
        db = conn.get_database(vaspdbinsert_params["database"])
        db.authenticate(vaspdbinsert_params["user"],
                        vaspdbinsert_params["password"])

        surface_properties = db[collection]
        collection_mpids = surface_properties.distinct("material_id")

        for mpid in collection_mpids:
            spa = SpacegroupAnalyzer(mprester.get_structure_by_material_id(mpid))
            if spa.get_crystal_system() == "cubic":
                cubic.append(mpid)
            else:
                non_cubic.append(mpid)

    elif specific:
        for mpid in specific:
            spa = SpacegroupAnalyzer(mprester.get_structure_by_material_id(mpid))
            if spa.get_crystal_system() == "cubic":
                cubic.append(mpid)
            else:
                non_cubic.append(mpid)
    else:
        for el in Element:

            if Element(el).group in [17, 18]:
                continue
            if str(el) in ["H", "Po", "At", "Fr", "Ra", "N", "O"]:
                continue
            if Element(el).Z > 94:
                continue

            entries = mprester.get_entries(str(el), inc_structure="Final",
                                           property_data=["material_id", "e_above_hull"])
            for entry in entries:
                spa = SpacegroupAnalyzer(entry.structure)
                ucell = spa.get_conventional_standard_structure()
                mpid = entry.data["material_id"]

                if mpid in avoid:
                    continue
                elif entry.data["e_above_hull"] > less_than_ehull:
                    continue
                elif len(ucell) > limit_atoms:
                    continue
                else:
                    if spa.get_crystal_system() == "cubic":
                        cubic.append(mpid)
                    else:
                        non_cubic.append(mpid)

    def run_wf(element_and_mpids, max_index, slab=1):

        wf = SurfaceWorkflowManager(elements_and_mpids=element_and_mpids,
                                    slab_size=10, vac_size=10,
                                    check_exists=True,
                                    **vaspdbinsert_params)
        workflows = wf.from_max_index(max_index, max_normal_search=1,
                                      get_bulk_e=True)[slab]

        number_of_fws = workflows.launch_workflow(user_incar_settings=user_incar_settings,
                                                  job=job, scratch_dir=scratch_dir, gpu=gpu,
                                                  launchpad_dir=launchpad_dir)

        return number_of_fws

    number_of_fws = 0

    number_of_fws += run_wf(cubic, 3, slab=0)
    number_of_fws += run_wf(cubic, 3, slab=1)
    number_of_fws += run_wf(non_cubic, 2, slab=0)
    number_of_fws += run_wf(non_cubic, 2, slab=1)

    print("TOTAL NUMBER OF FIREWORKS: %s" %(number_of_fws))

class SurfaceWorkflowManager(object):

    """
        Initializes the workflow manager by taking in a list of
        formulas/mpids or a dictionary with the formula as the key referring
        to a list of miller indices.
    """

    def __init__(self, elements_and_mpids=[], indices_dict=None, ucell_dict={},
                 slab_size=10, vac_size=10, symprec=0.001, angle_tolerance=5,
                 ucell_indices_dict={}, bonds=None, max_broken_bonds=0, bondlength=None,
                 check_exists=True, debug=False, verbose=True,
                 host=None, port=None, user=None, password=None,
                 collection="surface_tasks",  database=None):

        """
            Args:
                elements_and_mpids ([str, ...]): A list of compounds or elements to create
                    slabs from. Must be a string that can be searched for with MPRester.
                    Either list_of_elements or indices_dict has to be entered in.
                indices_dict ({element(str): [[h,k,l], ...]}): A dictionary of
                    miller indices corresponding to the composition formula
                    (key) to transform into a list of slabs. Either list_of_elements
                    or indices_dict has to be entered in.
                ucell_dict ({some name or mpid: structure, ...}): A dictionary of
                    conventional unit cells with custom names as keys. Will get a
                    indices_dict using max index
                ucell_indices_dict ({some name or mpid: {"ucell": structure, "hkl_list": [(), ...]} ...}):
                    A dictionary of conventional unit cells and miller indices with custom
                    names as keys. Behave similarly to indices_dict
                angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
                symprec (float): See SpaceGroupAnalyzer in analyzer.py
                fail_safe (bool): Check for slabs/bulk structures with
                    more than 200 atoms (defaults to True)
                reset (bool): Reset your launchpad (defaults to False)
                check_exists (bool): Check if slab/bulk calculation
                    has already been completed
                debug (bool): Used by unit test to run workflow
                    without having to run custodian
                host (str): For database insertion
                port (int): For database insertion
                user (str): For database insertion
                password (str): For database insertion
                collection (str): For database insertion
                database (str): For database insertion
        """

        unit_cells_dict = {}

        # This initializes the REST adaptor. Put your own API key in.
        if "MAPI_KEY" not in os.environ:
            apikey = input('Enter your api key (str): ')
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
                polymorph_order = float("nan")
                mpid = copy.copy(el)

            elif ucell_indices_dict:
                conv_unit_cell = ucell_indices_dict[el]["ucell"]
                spa = SpacegroupAnalyzer(conv_unit_cell, symprec=symprec,
                                         angle_tolerance=angle_tolerance)
                polymorph_order = float("nan")
                mpid = copy.copy(el)
                new_indices_dict[mpid] = ucell_indices_dict[mpid]["hkl_list"]
            else:
                entries = mprest.get_entries(el, inc_structure="final")

                if verbose:
                    print("# of entries for %s: " %(el), len(entries))

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

                if verbose:
                    print(conv_unit_cell)
                    print(spa.get_spacegroup_symbol())

            # Get a dictionary of different properties for a particular material
            unit_cells_dict[mpid] = {"ucell": conv_unit_cell,
                                     "spacegroup": spa.get_spacegroup_symbol(),
                                     "polymorph": polymorph_order}

            if verbose:
                print(el)

        self.apikey = apikey
        self.vaspdbinsert_params = vaspdbinsert_params
        self.symprec = symprec
        self.angle_tolerance = angle_tolerance
        self.unit_cells_dict = unit_cells_dict
        self.indices_dict = new_indices_dict
        self.ssize = slab_size
        self.vsize = vac_size
        self.surface_query_engine = QueryEngine(**vaspdbinsert_params)
        self.check_exists = check_exists
        self.verbose = verbose
        self.debug = debug
        self.mprester = mprest
        self.bonds = bonds
        self.max_broken_bonds = max_broken_bonds
        self.bondlength=bondlength

    def from_max_index(self, max_index, max_normal_search=1,
                       max_only=False, get_bulk_e=True):

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
                get_symmetrically_distinct_miller_indices(self.unit_cells_dict[mpid]['ucell'],
                                                          max_index)

            if self.verbose:
                print('# ', mpid)

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
            return self.check_existing_entries(miller_dict,
                                               max_broken_bonds=self.max_broken_bonds,
                                               bonds=self.bonds,
                                               bondlength=self.bondlength,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_broken_bonds=self.max_broken_bonds,
                                         bonds=self.bonds, bondlength=self.bondlength,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e, debug=self.debug)

    def from_list_of_indices(self, list_of_indices, max_normal_search=1, get_bulk_e=True):

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
                                               max_broken_bonds=self.max_broken_bonds,
                                               bonds=self.bonds, bondlength=self.bondlength,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(miller_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_broken_bonds=self.max_broken_bonds,
                                         bonds=self.bonds, bondlength=self.bondlength,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e, debug=self.debug)

    def from_indices_dict(self, max_normal_search=1, get_bulk_e=True):

        """
            Class method to create a surface workflow with a dictionary with the keys
            being the formula of the unit cells we want to create slabs from which
            will refer to a list of miller indices.
            eg. indices_dict={'Fe': [[1,1,0]], 'LiFePO4': [[1,1,1], [2,2,1]]}
        """

        if self.check_exists:
            return self.check_existing_entries(self.indices_dict,
                                               max_broken_bonds=self.max_broken_bonds,
                                               bonds=self.bonds, bondlength=self.bondlength,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(self.indices_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize,
                                         max_broken_bonds=self.max_broken_bonds,
                                         bonds=self.bonds, bondlength=self.bondlength,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e, debug=self.debug)

    def from_termination_analysis(self, max_index, max_normal_search=1, get_bulk_e=True,
                                  bond_length_tol=0.1, max_term=6, min_surfaces=3):

        indices_dict = {}
        for mpid in self.unit_cells_dict.keys():

            ucell = self.unit_cells_dict[mpid]["ucell"]
            bonds, bondlength, max_broken_bonds = termination_analysis(ucell, max_index, max_term=max_term,
                                                                       bond_length_tol=bond_length_tol,
                                                                       min_surfaces=min_surfaces)

            all_slabs = generate_all_slabs(ucell, max_index, 10, 10, bonds=bonds,
                                           max_broken_bonds=max_broken_bonds,
                                           center_slab=True, max_normal_search=1)
            miller_list = []
            for slab in all_slabs:
                if slab.miller_index not in miller_list:
                    miller_list.append(slab.miller_index)

            indices_dict[mpid] = miller_list

        if self.check_exists:
            return self.check_existing_entries(indices_dict, bonds=bonds,
                                               bondlength=bondlength,
                                               max_broken_bonds=max_broken_bonds,
                                               max_normal_search=max_normal_search)
        else:
            return CreateSurfaceWorkflow(indices_dict, self.unit_cells_dict,
                                         self.vaspdbinsert_params,
                                         self.ssize, self.vsize, bonds=bonds,
                                         bondlength=bondlength,
                                         max_broken_bonds=max_broken_bonds,
                                         max_normal_search=max_normal_search,
                                         get_bulk_e=get_bulk_e, debug=self.debug)

    def check_existing_entries(self, miller_dict, max_normal_search=1,
                               bondlength=None, bonds=None, max_broken_bonds=0):

        # Checks if a calculation is already in the DB to avoid
        # calculations that are already finish and creates workflows
        # with structures that have not been calculated yet.

        criteria = {'state': 'successful'}

        # To keep track of which calculations
        # are done and can be skipped
        calculate_with_bulk, calculate_with_slab_only = {}, {}
        total_calculations, total_calcs_finished, \
        total_calcs_with_bulk, total_calcs_with_nobulk = 0, 0, 0, 0

        for mpid in miller_dict.keys():
            total_calculations += len(miller_dict[mpid])
            for hkl in miller_dict[mpid]:
                m = max(abs(hkl[0]), abs(hkl[1]), abs(hkl[2]))
                criteria['structure_type'] = 'oriented_unit_cell'
                criteria['material_id'] = mpid

                miller_handler = GetMillerIndices(self.unit_cells_dict[mpid]["ucell"], m)

                found_hkl_slab = False
                found_hkl_bulk = False
                for miller_index in miller_handler.get_symmetrically_equivalent_miller_indices(hkl):
                    criteria['miller_index'] = miller_index

                    # Check if the oriented unit cell
                    # has already been calculated

                    ucell_entries = self.surface_query_engine.get_entries(criteria,
                                                                          inc_structure="Final")

                    if ucell_entries:
                        found_hkl_bulk = True

                    criteria['structure_type'] = 'slab_cell'
                    slab_entries = self.surface_query_engine.get_entries(criteria, inc_structure="Final")

                    if slab_entries:
                        found_hkl_slab = True

                if found_hkl_bulk:
                    if self.verbose:
                        print('%s %s oriented unit cell already calculated, ' \
                              'now checking for existing slab' %(mpid, hkl))

                    # Check if slab calculations are complete if the
                    # oriented unit cell has already been calculated

                    if found_hkl_slab:
                        continue

                    else:
                        # No slab calculations found, insert slab calculations
                        if mpid not in calculate_with_slab_only.keys():
                            calculate_with_slab_only[mpid] = []

                        if self.verbose:
                            print('%s %s slab cell not in DB, ' \
                                  'will insert calculation into WF' %(mpid, hkl))

                        calculate_with_slab_only[mpid].append(hkl)
                        total_calcs_with_nobulk += 1
                else:

                    # Insert complete calculation for oriented ucell and
                    # slabs if no oriented ucell calculation has been found
                    if mpid not in calculate_with_bulk.keys():
                        calculate_with_bulk[mpid] = []

                    if self.verbose:
                        print('%s %s oriented unit  cell not in DB, ' \
                              'will insert calculation into WF' %(mpid, hkl))

                    calculate_with_bulk[mpid].append(hkl)
                    total_calcs_with_bulk +=1

        # Get the parameters for CreateSurfaceWorkflow. Will create two separate WFs,
        # one that calculates slabs only and one that calculates slabs and oriented ucells

        wf_kwargs = {'unit_cells_dict': self.unit_cells_dict,
                     'vaspdbinsert_params': self.vaspdbinsert_params,
                     'ssize': self.ssize, 'vsize': self.vsize,
                     'max_normal_search': max_normal_search}

        with_bulk = CreateSurfaceWorkflow(calculate_with_bulk, debug=self.debug,
                                          get_bulk_e=True, bonds=bonds,
                                          bondlength=bondlength,
                                          max_broken_bonds=max_broken_bonds, **wf_kwargs)

        with_slab_only = CreateSurfaceWorkflow(calculate_with_slab_only, debug=self.debug,
                                               get_bulk_e=False, bonds=bonds,
                                               bondlength=bondlength,
                                               max_broken_bonds=max_broken_bonds, **wf_kwargs)

        status = ["total number of indices: ",
                  "total number of calculations with no bulk: ",
                  "total number of calculations with bulk: ",
                  "total number of calculations already finished: ", ]
        if self.verbose:
            print(status[0], total_calculations)
            print(status[1], total_calcs_with_nobulk, calculate_with_slab_only)
            print(status[2], total_calcs_with_bulk, calculate_with_bulk)
            print(status[3], total_calcs_finished)
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
                 ssize, vsize, max_normal_search=1, debug=False, bonds=None,
                 max_broken_bonds=0, bondlength=None, get_bulk_e=True):

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
        self.get_bulk_e = get_bulk_e
        self.debug = debug
        self.max_broken_bonds = max_broken_bonds
        self.bonds = bonds
        self.bondlength = bondlength


    def launch_workflow(self, k_product=50, job=None, gpu=False,
                        user_incar_settings=None, potcar_functional='PBE', oxides=False,
                        additional_handlers=[], limit_sites_bulk=199, limit_sites_slab=199,
                        limit_sites_at_least_slab=0, limit_sites_at_least_bulk=0,
                        scratch_dir=None, launchpad_dir=""):

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
                    FrozenJobErrorHandler(output_filename="OSZICAR", timeout=3600)]

        if additional_handlers:
            handlers.extend(additional_handlers)

        scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir

        cust_params = {"scratch_dir": os.path.join(scratch_dir, os.environ["USER"]),
                       "jobs": job.double_relaxation_run(job.vasp_cmd,
                                                         auto_npar=False),
                       "handlers": handlers,
                       "max_errors": 10}  # will return a list of jobs
                                           # instead of just being one job
        cwd = os.getcwd()
        fws, fw_ids = [], []
        for mpid in self.miller_dict.keys():

            # Enumerate through all compounds in the dictionary,
            # the key is the compositional formula of the compound

            print(mpid)

            kwargs_GenerateFwsTasks = {"miller_list": self.miller_dict[mpid],
                                       "unit_cells_dict": self.unit_cells_dict[mpid],
                                       "ssize": self.ssize, "vsize":self.vsize,
                                       "max_normal_search": self.max_normal_search,
                                       "vaspdbinsert_params": self.vaspdbinsert_params,
                                       "cust_params": cust_params,
                                       "get_bulk_e": self.get_bulk_e, "mpid": mpid,
                                       "user_incar_settings": user_incar_settings,
                                       "oxides": oxides, "k_product": k_product,
                                       "gpu": gpu, "debug": self.debug,
                                       "potcar_functional": potcar_functional,
                                       "limit_sites_at_least_slab": limit_sites_at_least_slab,
                                       "limit_sites_slab": limit_sites_slab,
                                       "limit_sites_bulk": limit_sites_bulk,
                                       "limit_sites_at_least_bulk": limit_sites_at_least_bulk,
                                       "max_broken_bonds": self.max_broken_bonds,
                                       "bondlength": self.bondlength, "cwd": cwd}

            conv_ucell = get_conventional_ucell(mpid, from_mapi=False,
                                                qe=self.surface_query_engine)
            if conv_ucell:
                print("Found relaxed conventional unit cell, "
                      "will construct all oriented ucells from this")
                self.unit_cells_dict[mpid]["ucell"] = conv_ucell
                kwargs_GenerateFwsTasks["unit_cells_dict"] = self.unit_cells_dict[mpid]

                tasks = [GenerateFwsTask(**kwargs_GenerateFwsTasks)]

            else:
                print("No relaxed conventional unit cell available, "
                      "need to calculate relaxed ucell first")
                oriented_uc = self.unit_cells_dict[mpid]["ucell"]
                folderbulk = '%s_%s_%s_k%s_s%sv%s_%s%s%s' %(oriented_uc.composition.reduced_formula,
                                                            mpid,'bulk', k_product, self.ssize,
                                                            self.vsize, 0,0,1)

                task_kwargs = {"folder": folderbulk, "cwd": cwd, "debug": self.debug}
                input_task_kwargs = task_kwargs.copy()
                input_task_kwargs.update({"user_incar_settings": user_incar_settings,
                                          "k_product": k_product, "gpu": gpu, "oxides": oxides,
                                          "potcar_functional": potcar_functional})

                tasks = [WriteUCVaspInputs(oriented_ucell=oriented_uc, **input_task_kwargs),
                         RunCustodianTask(custodian_params=cust_params, **task_kwargs),
                         VaspSlabDBInsertTask(struct_type="oriented_unit_cell",
                                              miller_index=(0,0,1), mpid=mpid,
                                              unit_cell_dict=self.unit_cells_dict[mpid],
                                              vaspdbinsert_parameters=self.vaspdbinsert_params,
                                              **task_kwargs),
                         GenerateFwsTask(**kwargs_GenerateFwsTasks)]

            fw = Firework(tasks, name="%s_%s" %(str(self.unit_cells_dict[mpid]["ucell"][0].specie), mpid))
            fw_ids.append(fw.fw_id)
            fws.append(fw)

        wf = Workflow(fws, name='Surface Calculations')
        launchpad = LaunchPad.from_file(os.path.join(os.environ["HOME"],
                                                     launchpad_dir,
                                                     "my_launchpad.yaml"))
        launchpad.add_wf(wf)

        number_of_fws = len(fws)
        return number_of_fws

def atomic_energy_workflow(host=None, port=None, user=None, password=None, database=None,
                           collection="Surface_Collection", latt_a=16, kpoints=1, job=None,
                           scratch_dir=None, additional_handlers=[], launchpad_dir="",
                           elements=[], user_incar_settings={}, gpu=False):

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

    handlers = [NonConvergingErrorHandler(change_algo=True),
                UnconvergedErrorHandler(),
                PotimErrorHandler(),
                PositiveEnergyErrorHandler(),
                VaspErrorHandler(),
                FrozenJobErrorHandler(output_filename="OSZICAR",
                                      timeout=3600)]
    if additional_handlers:
        handlers.extend(additional_handlers)

    scratch_dir = "/scratch2/scratchdirs/" if not scratch_dir else scratch_dir

    cust_params = {"scratch_dir":
                       os.path.join(scratch_dir,
                                    os.environ["USER"]),
                   "jobs": job.double_relaxation_run(job.vasp_cmd,
                                                     auto_npar=False),
                   "handlers": handlers,
                   "max_errors": 10}

    fws = []
    for el in elements:
        folder_atom = '%s_isolated_atom_%s_k%s' %(el, latt_a, kpoints)
        cwd = os.getcwd()

        tasks = [WriteAtomVaspInputs(atom=el, folder=folder_atom, cwd=cwd,
                                     latt_a=latt_a, kpoints=kpoints, gpu=gpu,
                                     user_incar_settings=user_incar_settings),
                 RunCustodianTask(folder=folder_atom, cwd=cwd,
                                  custodian_params=cust_params),
                 VaspSlabDBInsertTask(struct_type="isolated_atom",
                                      folder=folder_atom, cwd=cwd,
                                      miller_index=None, mpid=None,
                                      conventional_spacegroup=None,
                                      conventional_unit_cell=None,
                                      isolated_atom=el,
                                      polymorph=None,
                                      unit_cell_dict={"polymoph": None, "ucell": None,
                                                      "spacegroup": None},
                                      vaspdbinsert_parameters=vaspdbinsert_params)]

        fws.append(Firework(tasks, name=folder_atom))

    wf = Workflow(fws, name='Surface Calculations')
    launchpad.add_wf(wf)

def get_bond_length(structure):

    list_of_bonds = []
    voronoi_connections = VoronoiConnectivity(structure, cutoff=5)
    list_of_bond_pairs = voronoi_connections.get_connections()
    for pair in list_of_bond_pairs:
        if pair[2] !=0:
            list_of_bonds.append(pair[2])
    return min(list_of_bonds)

def termination_analysis(structure, max_index, bond_length_tol=0.1,
                         max_term=6, min_surfaces=2):

    """
    Determines the most stable surfaces based on broken bond rules. This
        should only be used for structures with unreasonable number of
        terminations (e.g. Mn, S, Se, P, B) making the calculation of their
        surfaces computationally intensive. For now, let's assume we're
        dealing with elemental solids.

    Args:
        structure (Structure): Initial input structure. Note that to
                ensure that the miller indices correspond to usual
                crystallographic definitions, you should supply a
                conventional unit cell structure.
        max_index (int): The maximum Miller index to go up to.

        search_broken_bonds (int): The number of broken bonds to limit. The
            algorithm will increase the number of allowed broken bonds to
            find stable surfaces up until this number.

    Returns:
        Slab parameters:
            bonds ({("element1", "element2"): int})
            max_broken_bonds (int)
    """

    bond_length = get_bond_length(structure)+bond_length_tol
    el = str(structure[0].specie)
    bonds = {tuple((el, el)): bond_length}

    max_broken_bonds = 0
    term_count = 0
    last_num_terms=False
    while term_count <= max_term:

        all_slabs = generate_all_slabs(structure, max_index, 10,
                                       10, max_normal_search=1,
                                       symmetrize=True, bonds=bonds,
                                       max_broken_bonds=max_broken_bonds)
        slabdict = {}
        for slab in all_slabs:
            if slab.miller_index not in slabdict.keys():
                slabdict[slab.miller_index] = []
            slabdict[slab.miller_index].append(slab)

        max_terms = [len(slabdict[hkl]) for hkl in slabdict.keys()]
        if max_terms:
            term_count = max(max_terms)
        max_broken_bonds += 1

        if len(slabdict.keys()) >= min_surfaces:
            break
        print(1)

        last_num_terms = max_terms

    if not last_num_terms:
        max_broken_bonds -= 1
    if last_num_terms != 0:
        max_broken_bonds -= 1
    print(bonds, bond_length, max_broken_bonds)
    return bonds, bond_length, max_broken_bonds


