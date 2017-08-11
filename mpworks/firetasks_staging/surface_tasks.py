## for Surface Energy Calculation
from __future__ import division, unicode_literals

__author__ = "Richard Tran, Zihan Xu"
__version__ = "0.1"
__email__ = "rit634989@gmail.com"
__date__ = "6/2/15"

import numpy as np
import itertools
import cStringIO
import json
import os
import logging
import warnings

from fireworks.core.firework import FireTaskBase, FWAction, Firework
from fireworks import explicit_serialize

from custodian.custodian import Custodian

from matgendb.creator import VaspToDbTaskDrone
from matgendb import QueryEngine

from pymatgen.io.vasp.sets import MVLSlabSet
from pymatgen.io.vasp.outputs import Incar, Outcar, Oszicar
from pymatgen.core.surface import SlabGenerator
from pymatgen.core.structure import Structure, Lattice
from pymatgen.analysis.structure_analyzer import RelaxationAnalyzer, VoronoiConnectivity
from pymatgen.util.coord_utils import in_coord_list
from pymatgen import MPRester
from pymatgen.symmetry.groups import SpaceGroup
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.analysis.wulff import WulffShape

from pymongo import MongoClient
from bson import binary
from monty.json import MontyDecoder
from functools import reduce
from fractions import gcd

logger = logging.getLogger(__name__)


class EntryError(Exception):
    def __init__(self, value):
        self.value = value

EV_PER_ANG2_TO_JOULES_PER_M2 = 16.0217656

# This initializes the REST adaptor. Put your own API key in.
if "MAPI_KEY" not in os.environ:
    apikey = input('Enter your api key (str): ')
else:
    apikey = os.environ["MAPI_KEY"]

mprester = MPRester(apikey)

"""
Firework tasks
"""

@explicit_serialize
class VaspSlabDBInsertTask(FireTaskBase):

    """
        Inserts a single vasp calculation in a folder into a
        DB. Also inserts useful information pertaining
        to slabs and oriented unit cells.
    """

    required_params = ["vaspdbinsert_parameters",
                       "struct_type", "folder","miller_index",
                       "cwd", "unit_cell_dict"]
    optional_params = ["surface_area", "shift", "debug", "diatomic",
                       "vsize", "ssize", "isolated_atom", "mpid"]

    def run_task(self, fw_spec):

        """
            Required Parameters:
                host (str): See SurfaceWorkflowManager in surface_wf.py
                port (int): See SurfaceWorkflowManager in surface_wf.py
                user (str): See SurfaceWorkflowManager in surface_wf.py
                password (str): See SurfaceWorkflowManager in surface_wf.py
                database (str): See SurfaceWorkflowManager in surface_wf.py
                collection (str): See SurfaceWorkflowManager in surface_wf.py
                mpid (str): The Materials Project ID associated with the
                    initial structure used to build the slab from
                struct_type (str): either oriented_unit_cell or slab_cell
                miller_index (list): Miller Index of the oriented
                    unit cell or slab
                loc (str path): Location of the outputs of
                    the vasp calculations
                cwd (str): Current working directory
                conventional_spacegroup (str): The spacegroup of the structure
                    asscociated with the MPID input
                polymorph (str): The rank of the  polymorph of the structure
                    associated with the MPID input, 0 being the ground state
                    polymorph.
            Optional Parameters:
                surface_area (float): surface area of the slab, obtained
                    from slab object before relaxation
                shift (float): A shift value in Angstrom that determines how
                    much a slab should be shifted. For determining number of
                    terminations, obtained from slab object before relaxation
                vsize (float): Size of vacuum layer of slab in Angstroms,
                    obtained from slab object before relaxation
                ssize (float): Size of slab layer of slab in Angstroms,
                    obtained from slab object before relaxation
                isolated_atom (str): Specie of the structure used to
                    calculate the energy of an isolated atom (for cohesive
                    energy calculations)
        """

        # Get all the optional/required parameters
        dec = MontyDecoder()
        struct_type = dec.process_decoded(self.get("struct_type"))
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        surface_area = dec.process_decoded(self.get("surface_area", None))
        shift = dec.process_decoded(self.get("shift", None))
        vsize = dec.process_decoded(self.get("vsize", None))
        ssize = dec.process_decoded(self.get("ssize", None))
        miller_index = dec.process_decoded(self.get("miller_index"))
        mpid = dec.process_decoded(self.get("mpid", None))
        diatomic = dec.process_decoded(self.get("diatomic", False))
        unit_cell_dict = dec.process_decoded(self.get("unit_cell_dict"))
        isolated_atom = dec.process_decoded(self.get("isolated_atom", None))
        vaspdbinsert_parameters = \
            dec.process_decoded(self.get("vaspdbinsert_parameters"))
        debug = dec.process_decoded(self.get("debug", False))

        qe = QueryEngine(**vaspdbinsert_parameters)

        warnings = []

        if struct_type != "isolated_atom":
            # Check if the spacegroup queried from MP API consistent
            # with the one calculated from the queried structure
            spacegroup = unit_cell_dict["spacegroup"]
            conventional_unit_cell = unit_cell_dict["ucell"]
            spa = SpacegroupAnalyzer(conventional_unit_cell,
                                     symprec=0.001, angle_tolerance=5)
            calculated_sg = spa.get_spacegroup_symbol()

            if str(calculated_sg) != spacegroup:
                warnings.append("api_mp_spacegroup_inconsistent")

        if struct_type == "slab_cell":

            optional_data = ["final_structure", "initial_structure"]
            ucell_entry = qe.get_entries({'material_id': mpid, 'miller_index': miller_index,
                                          'structure_type': 'oriented_unit_cell'},
                                         optional_data=optional_data)[0]

            init_bulk = Structure.from_dict(ucell_entry.data["initial_structure"])
            fin_bulk = Structure.from_dict(ucell_entry.data["final_structure"])

            # Analyze bulk relaxations for possible
            # warning signs, too much volume relaxation
            relaxation = RelaxationAnalyzer(init_bulk, fin_bulk)
            if abs(relaxation.get_percentage_volume_change()*100) > 1:
                warnings.append("|bulk_vol_rel|>1%")

            initial = Structure.from_file(os.path.join(cwd, folder, 'POSCAR'))
            final = Structure.from_file(os.path.join(cwd, folder, 'CONTCAR.relax2.gz'))

            # Analyze slab site relaxations for possible
            # warning signs, too much site relaxation
            total_percent = []
            percent_dict = {}

            # Determine the max bond length to determine
            # relaxation based on the conventional unit cell
            connections = VoronoiConnectivity(conventional_unit_cell).get_connections()
            all_dist = []
            for connection in connections:
                all_dist.append(connection[2])

            relaxation_analyzer = RelaxationAnalyzer(initial, final)
            rel_dict = relaxation_analyzer.get_percentage_bond_dist_changes(max_radius=max(all_dist)+0.1)
            for i in rel_dict.keys():
                site_per = []

                for ii in rel_dict[i].keys():
                    site_per.append(abs(rel_dict[i][ii]))
                    total_percent.append(abs(rel_dict[i][ii]))
                percent_dict[i]=100*np.mean(site_per)

            # Create a warning if any bonds in
            # the structure is greater than 5-10%
            if any(10 < i for i in percent_dict.values()):
                warnings.append("|slab_site_rel|>10%")
            elif any(5 < i < 10 for i in percent_dict.values()):
                warnings.append("|slab_site_rel|>5%")

            # Check the symmetry of surfaces (unrelaxed)
            sg = SpacegroupAnalyzer(initial, symprec=1E-3)
            pg = sg.get_point_group()
            laue = ["-1", "2/m", "mmm", "4/m", "4/mmm",
                    "-3", "-3m", "6/m", "6/mmm", "m-3", "m-3m"]
            if str(pg) not in laue:
                warnings.append("unequivalent_surfaces")

            # For check negative surface energy
            e_per_atom = ucell_entry.energy_per_atom
            # Find out if an entry for this slab already exists.
            # If so, see if the slab at shift=0 has been calculated,
            # then calculate all other terminations besides c=0
            final_energy = Oszicar(os.path.join(cwd,folder,'OSZICAR.relax2.gz')).final_energy
            surface_e = final_energy - e_per_atom*len(initial)
            if surface_e < 0:
                warnings.append("negative_surface_energy")

            # Check if the EDIFF was changed (this will only happen as
            # a last resort for bypassing the NonConvergenceError)
            incar = Incar.from_file(os.path.join(cwd, folder, 'INCAR.relax2.gz'))
            if incar["EDIFF"] > 1e-06:
                warnings.append("ediff_is_1e-05")

        name = folder

        # Addtional info relating to slabs
        additional_fields = {
                             "author": os.environ.get("USER"),
                             # User that ran the calculation
                             "structure_type": struct_type,
                             "final_incar": Incar.from_file("./INCAR.relax2.gz"),
                             # Final incar parameters after custodian fixes have been applied.
                             # Useful for creating the slab incar parameters based on the
                             # parameters set for the oriented unit cell
                             "final_magnetization": Outcar("./OUTCAR.relax2.gz").magnetization,
                             # The final magnetization acquired from the outcar.
                             # Useful for slab magmom inheriting the magnetization
                             # calculated from the oriented ucell.
                            "calculation_name":  name,
                            "warnings": warnings
                           }

        # Add mpid as optional so we won't get None
        # when looking for mpid of isolated atoms
        if struct_type != "isolated_atom":
            additional_fields["miller_index"] = miller_index
            additional_fields["surface_area"] = surface_area
            additional_fields["shift"] = shift
            additional_fields["vac_size"] = vsize
            additional_fields["slab_size"] = ssize
            additional_fields["material_id"] = mpid
            additional_fields["conventional_spacegroup"] = spacegroup
            additional_fields["polymorph"] =  unit_cell_dict["polymorph"]
        else:
            if diatomic:
                additional_fields["isolated_atom"] = isolated_atom + "2"
            else:
                additional_fields["isolated_atom"] = isolated_atom
        if mpid:
            additional_fields["material_id"] = mpid

        drone = VaspToDbTaskDrone(use_full_uri=False,
                                  additional_fields=additional_fields,
                                  **vaspdbinsert_parameters)
        drone.assimilate(os.path.join(cwd, folder))

        if struct_type != "isolated_atom":
            # Now we update the surface properties collection
            post_process_updater = UpdateRepositoriesAndDBs(vaspdbinsert_parameters)
            post_process_updater.insert_surface_property_entry(mpid)
            post_process_updater.insert_wulff_entry(mpid)

@explicit_serialize
class WriteUCVaspInputs(FireTaskBase):
    """
        Writes VASP inputs for an oriented unit cell
    """

    required_params = ["oriented_ucell", "folder", "cwd", "potcar_functional"]
    optional_params = ["user_incar_settings", "oxides", "limit_sites",
                       "k_product", "gpu", "debug"]

    def run_task(self, fw_spec):

        """
            Required Parameters:
                oriented_unit_cell (Structure): Generated with surface.py
                folder (str path): Location where vasp inputs
                    are to be written
                cwd (str): Current working directory
            Optional Parameters:
                user_incar_settings (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                k_product (int): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (str): See launch_workflow() method in
                    CreateSurfaceWorkflow class
        """
        dec = MontyDecoder()
        oriented_ucell = dec.process_decoded(self.get("oriented_ucell"))
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        gpu = dec.process_decoded(self.get("gpu", False))

        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_functional"))
        oxides = \
            dec.process_decoded(self.get("oxides", False))
        debug = dec.process_decoded(self.get("debug", False))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings", {}))
        mplb = MVLSlabSet(oriented_ucell, bulk=True, gpu=gpu,
                          user_incar_settings=user_incar_settings,
                          k_product=k_product,
                          potcar_functional=potcar_functional)

        mplb.write_input(os.path.join(cwd, folder))


@explicit_serialize
class WriteSlabVaspInputs(FireTaskBase):
    """
        Adds dynamicism to the workflow by creating addition Fireworks for each
        termination of a slab or just one slab with shift=0. First the vasp
        inputs of a slab is created, then the Firework for that specific slab
        is made with a RunCustodianTask and a VaspSlabDBInsertTask
    """
    required_params = ["folder", "cwd", "custodian_params", "potcar_functional", "mpid",
                       "vaspdbinsert_parameters", "miller_index", "unit_cell_dict"]
    optional_params = ["min_slab_size", "min_vacuum_size", "user_incar_settings",
                       "limit_sites", "limit_sites_at_least", "oxides", "k_product",
                       "gpu", "debug", "bondlength", "max_broken_bonds"]

    def run_task(self, fw_spec):

        """
            Required Parameters:
                folder (str path): Location where vasp inputs
                    are to be written
                cwd (str path): Current working directory
                custodian_params (dict **kwargs): Contains the job and the
                    scratch directory for a custodian run
                vaspdbinsert_parameters (dict **kwargs): Contains
                    informations needed to acess a DB, eg, host,
                    port, password etc.
                miller_index (list): Miller Index of the oriented
                    unit cell or slab

            Optional Parameters:
                min_vac_size (float): Size of vacuum layer of slab in Angstroms
                min_slab_size (float): Size of slab layer of slab in Angstroms
                user_incar_settings (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                k_product (int): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (str): See launch_workflow() method in
                    CreateSurfaceWorkflow class
        """
        dec = MontyDecoder()
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        custodian_params = dec.process_decoded(self.get("custodian_params"))
        vaspdbinsert_parameters = \
            dec.process_decoded(self.get("vaspdbinsert_parameters"))

        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_functional"))
        min_slab_size = dec.process_decoded(self.get("min_slab_size", 10))
        min_vacuum_size = dec.process_decoded(self.get("min_vacuum_size", 10))
        miller_index = dec.process_decoded(self.get("miller_index"))
        mpid = dec.process_decoded(self.get("mpid"))
        oxides = dec.process_decoded(self.get("oxides", False))
        gpu = dec.process_decoded(self.get("gpu", False))
        bondlength = dec.process_decoded(self.get("bondlength", None))
        max_broken_bonds = dec.process_decoded(self.get("max_broken_bonds", 0))
        limit_sites = dec.process_decoded(self.get("limit_sites", 199))
        limit_sites_at_least = dec.process_decoded(self.get("limit_sites_at_least", 0))
        unit_cell_dict = dec.process_decoded(self.get("unit_cell_dict", 0))
        debug = dec.process_decoded(self.get("debug", False))
        user_incar_settings = dec.process_decoded(self.get("user_incar_settings", {}))

        el = str(unit_cell_dict["ucell"][0].specie)
        bonds = {(el, el): bondlength} if bondlength else None

        # Create slabs from the relaxed oriented unit cell. Since the unit
        # cell is already oriented with the miller index, entering (0,0,1)
        # into SlabGenerator is the same as obtaining a slab in the
        # orienetation of the original miller index.

        qe = QueryEngine(**vaspdbinsert_parameters)
        optional_data = ["state", "shift", "final_incar", "final_magnetization"]
        ucell_entry = qe.get_entries({'material_id': mpid, 'structure_type': 'oriented_unit_cell',
                                      'miller_index': miller_index}, inc_structure="Final",
                                      optional_data=optional_data)[0]

        relax_orient_uc = ucell_entry.structure

        # While loop ensures slab is at least 10 A
        bond_dist = 0
        while bond_dist <= 10:

            slabgen = SlabGenerator(relax_orient_uc, (0,0,1),
                                    min_slab_size=min_slab_size,
                                    min_vacuum_size=min_vacuum_size,
                                    center_slab=True,
                                    max_normal_search=max(miller_index),
                                    primitive=True)

            c_pos = []

            for site in slabgen.get_slabs(bonds=bonds,
                                          max_broken_bonds=max_broken_bonds)[0].frac_coords:
                c_pos.append(site[2])
            c_pos = sorted(c_pos)
            bond_dist = abs(c_pos[0]-c_pos[-1])*slabgen.get_slabs()[0].lattice.c
            if bond_dist < 10:
                min_slab_size += 5

        slab_list = slabgen.get_slabs(bonds=bonds, max_broken_bonds=max_broken_bonds)

        print('chemical formula', relax_orient_uc.composition.reduced_formula)
        print('mpid', mpid)
        print("Miller Index: ", miller_index)
        print(ucell_entry.data['state'])
	print(os.path.join(cwd, folder))

        # Check if ucell calculation was successful before doing slab calculation
        if ucell_entry.data['state'] != 'successful':
            print("%s bulk calculations were incomplete, cancelling FW" \
                  %(relax_orient_uc.composition.reduced_formula))
            return FWAction()

        print(ucell_entry.data['state'])

        FWs = []

        # Now create the slab(s) and ensure the surfaces are
        # symmeric and the ssize is at least that of min_slab_size
        new_slab_list, new_min_slab_size = check_termination_symmetry(slab_list, miller_index,
                                                                      min_slab_size,
                                                                      min_vacuum_size,
                                                                      relax_orient_uc)
        # If no stoichiometric/symmetric slab can be
        # generated, don't bother generating a fw
        if not new_slab_list:
            return

        # If any of the slab number of sites exceed the limit, complete this task
        exceeded_lim = [len(slab) > limit_sites for slab in new_slab_list]
        if any(exceeded_lim):
            return
        # If any of the slab number of sites less then the limit, complete this task
        exceeded_lim = [len(slab) < limit_sites_at_least for slab in new_slab_list]
        if any(exceeded_lim):
            return

        for slab in new_slab_list:

            new_folder = folder.replace('bulk', 'slab')+'_shift%s' \
                                                        %(slab.shift)

            mplb = MVLSlabSet(slab, user_incar_settings=user_incar_settings,
                              k_product=k_product, gpu=gpu,
                              potcar_functional=potcar_functional)
            mplb.write_input(os.path.join(cwd, new_folder))

            # Inherit the final magnetization of a slab
            # from the outcar of the ucell calculation.

            out_mag = ucell_entry.data["final_magnetization"]
            if not out_mag or out_mag[0]['tot'] < 0:
                warnings.warn("Magnetization not found in OUTCAR.relax2.gz, "
                              "may be incomplete, will set default magmom")
                if slab.composition.reduced_formula in ["Fe", "Co", "Ni"]:
                    out_mag = [{'tot': 5}]
                else:
                    out_mag = [{'tot': 0.6}]
            if out_mag[0]['tot'] == 0:
                warnings.warn("Magnetization is 0, "
                              "changing magnetization to non-zero")
                out_mag = [{'tot': 1E-15}]

            tot_mag = [mag['tot'] for mag in out_mag]
            magmom = np.mean(tot_mag)
            mag = [magmom]*len(slab)

            # Tries to build an incar from a previously calculated slab with a
            # different termination. Otherwise writes new INCAR file based on
            # changes made by custodian on the bulk's INCAR. Some parameters
            # may not be inherited from bulk, ie. IBRION is always initially
            # 2, NBANDS is turned off in slab calculations to avoid band
            # related errors, ISIF = 2 to prevent lattice relaxation in the slab.

            slab_entry = qe.get_entries({'material_id': mpid, 'structure_type': 'slab_cell',
                                         'miller_index': miller_index}, inc_structure=True,
                                        optional_data=optional_data)
            incar = slab_entry[0].data["final_incar"] if slab_entry else ucell_entry.data["final_incar"]
            incar = Incar.from_dict(incar)

            incar.__setitem__('MAGMOM', mag)

            # Set slab specific parameters not inherited from the ucell calculations

            incar.__setitem__('ISIF', 2)
            incar.__setitem__('AMIN', 0.01)
            incar.__setitem__('AMIX', 0.2)
            incar.__setitem__('BMIX', 0.001)
            incar.__setitem__('ISTART', 0)
            incar.__setitem__('NELMIN', 8)
            incar.__setitem__('IBRION', 2)
            incar.__setitem__('EDIFF', 1e-04)
            incar.__setitem__('LVTOT', True)

            if gpu:
                if "KPAR" not in incar.keys():
                    incar.__setitem__('KPAR', 1)
                if "NPAR" in incar.keys():
                    del incar["NPAR"]
            else:
                if "KPAR" in incar.keys():
                    del incar["KPAR"]

            if user_incar_settings:
                incar.update(user_incar_settings)

            if "NBANDS" in incar.keys():
                incar.pop("NBANDS")
            incar.write_file(os.path.join(cwd,new_folder,'INCAR'))

            fw = Firework([RunCustodianTask(folder=new_folder, cwd=cwd,
                                            custodian_params=custodian_params),
                           VaspSlabDBInsertTask(struct_type="slab_cell",
                                                folder=new_folder, cwd=cwd, shift=slab.shift,
                                                surface_area=slab.surface_area,
                                                vsize=min_vacuum_size,
                                                ssize=new_min_slab_size, miller_index=miller_index,
                                                mpid=mpid, unit_cell_dict=unit_cell_dict,
                                                vaspdbinsert_parameters=vaspdbinsert_parameters)],
                          name=new_folder)

            FWs.append(fw)

        # Skip this calculation if the surfaces aren't symmetric
        return FWAction(additions=FWs)


@explicit_serialize
class GenerateFwsTask(FireTaskBase):
    """
        Writes VASP inputs for an oriented unit cell
    """

    required_params = ["miller_list", "unit_cells_dict", "ssize", "vsize", "max_normal_search",
                       "vaspdbinsert_params", "cust_params", "get_bulk_e", "mpid", "cwd"]
    optional_params = ["user_incar_settings", "oxides", "k_product", "gpu", "debug",
                       "potcar_functional", "limit_sites_at_least_slab", "limit_sites_slab",
                       "limit_sites_bulk", "limit_sites_at_least_bulk", "max_broken_bonds",
                       "bondlength"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        miller_list = dec.process_decoded(self.get("miller_list"))
        unit_cells_dict = dec.process_decoded(self.get("unit_cells_dict"))
        ssize = dec.process_decoded(self.get("ssize"))
        vsize = dec.process_decoded(self.get("vsize"))
        max_normal_search = dec.process_decoded(self.get("max_normal_search"))
        vaspdbinsert_params = dec.process_decoded(self.get("vaspdbinsert_params"))
        cust_params = dec.process_decoded(self.get("cust_params"))
        get_bulk_e = dec.process_decoded(self.get("get_bulk_e"))
        mpid = dec.process_decoded(self.get("mpid"))
        cwd = dec.process_decoded(self.get("cwd"))

        user_incar_settings = dec.process_decoded(self.get("user_incar_settings", {}))
        oxides = dec.process_decoded(self.get("oxides", False))
        k_product = dec.process_decoded(self.get("k_product", 50))
        gpu = dec.process_decoded(self.get("gpu", False))
        debug = dec.process_decoded(self.get("debug", False))
        potcar_functional = dec.process_decoded(self.get("potcar_functional", "PBE"))
        limit_sites_at_least_slab = dec.process_decoded(self.get("limit_sites_at_least_slab", 0))
        limit_sites_slab = dec.process_decoded(self.get("limit_sites_slab", 199))
        limit_sites_bulk = dec.process_decoded(self.get("limit_sites_bulk", 199))
        limit_sites_at_least_bulk = dec.process_decoded(self.get("limit_sites_at_least_bulk", 0))
        max_broken_bonds = dec.process_decoded(self.get("max_broken_bonds", 0))
        bondlength = dec.process_decoded(self.get("bondlength", None))

        qe = QueryEngine(**vaspdbinsert_params)
        unit_cells_dict["ucell"] = get_conventional_ucell(mpid,
                                                          from_mapi=False,
                                                          qe=qe)

        FWs = []
        for miller_index in miller_list:
            # Enumerates through all miller indices we
            # want to create slabs of that compound from

            print(str(miller_index))

            slab = SlabGenerator(unit_cells_dict['ucell'], miller_index,
                                 ssize, vsize, primitive=False,
                                 max_normal_search=max_normal_search)
            oriented_uc = slab.oriented_unit_cell

            # The unit cell should not be exceedingly larger than the
            # conventional unit cell, reduced it down further if it is
            if len(oriented_uc)/len(unit_cells_dict['ucell']) > 20:
                reduced_slab = SlabGenerator(unit_cells_dict['ucell'], miller_index,
                                             ssize, vsize,
                                             lll_reduce=True, primitive=False)
                oriented_uc = reduced_slab.oriented_unit_cell

            if len(oriented_uc)> limit_sites_bulk:
                warnings.warn("UCELL EXCEEDED %s ATOMS!!!" %(limit_sites_bulk))
                continue
            if len(oriented_uc)< limit_sites_at_least_bulk:
                warnings.warn("UCELL LESS THAN %s ATOMS!!!" %(limit_sites_bulk))
                continue
            # This method only creates the oriented unit cell, the
            # slabs are created in the WriteSlabVaspInputs task.
            # WriteSlabVaspInputs will create the slabs from
            # the contcar of the oriented unit cell calculation

            folderbulk = '%s_%s_%s_k%s_s%sv%s_%s%s%s' %(oriented_uc.composition.reduced_formula,
                                                        mpid,'bulk', k_product, ssize,
                                                        vsize, str(miller_index[0]),
                                                        str(miller_index[1]), str(miller_index[2]))

            task_kwargs = {"folder": folderbulk, "cwd": cwd, "debug": debug}
            input_task_kwargs = task_kwargs.copy()
            input_task_kwargs.update({"user_incar_settings": user_incar_settings,
                                      "k_product": k_product, "gpu": gpu, "oxides": oxides,
                                      "potcar_functional": potcar_functional})
            tasks = []

            # This task is only initialized once the conventional unit
            # cell has been relaxed, skip the conventional unit cell
            # relaxation and go straight to the slab calculation
            miller_handler = GetMillerIndices(unit_cells_dict["ucell"], 1)
            if miller_handler.is_already_analyzed(miller_index, unique_millers=[(0,0,1)]):
                get_bulk_e = False
                miller_index = (0,0,1)

            if get_bulk_e:

                tasks.extend([WriteUCVaspInputs(oriented_ucell=oriented_uc, **input_task_kwargs),
                              RunCustodianTask(custodian_params=cust_params, **task_kwargs),
                              VaspSlabDBInsertTask(struct_type="oriented_unit_cell",
                                                   miller_index=miller_index, mpid=mpid,
                                                   unit_cell_dict=unit_cells_dict,
                                                   vaspdbinsert_parameters=vaspdbinsert_params,
                                                   **task_kwargs)])

            tasks.extend([WriteSlabVaspInputs(custodian_params=cust_params,
                                              vaspdbinsert_parameters=
                                              vaspdbinsert_params,
                                              miller_index=miller_index,
                                              min_slab_size=ssize,
                                              min_vacuum_size=vsize,
                                              bondlength= bondlength, mpid=mpid,
                                              max_broken_bonds=max_broken_bonds,
                                              unit_cell_dict=unit_cells_dict,
                                              limit_sites=limit_sites_slab,
                                              limit_sites_at_least=limit_sites_at_least_slab,
                                              **input_task_kwargs)])

            fw = Firework(tasks, name=folderbulk)
            FWs.append(fw)
            print(unit_cells_dict['spacegroup'])

        return FWAction(additions=FWs)


@explicit_serialize
class RunCustodianTask(FireTaskBase):

    """
        Runs Custodian.
    """

    required_params = ["folder", "cwd", "custodian_params"]
    optional_params = ["debug"]

    def run_task(self, fw_spec):

        """
            Required Parameters:
                dir (str path): directory containing the vasp inputs
                jobs (VaspJob): Contains the cmd needed to run vasp
            Optional Parameters:
                custodian_params (dict **kwargs): Contains the job and the
                    scratch directory for a custodian run
                handlers (list of custodian handlers): Defaults to empty list
        """

        dec = MontyDecoder()
        folder = dec.process_decoded(self['folder'])
        cwd = dec.process_decoded(self['cwd'])
        debug = dec.process_decoded(self.get("debug", False))

        print(os.path.join(cwd,folder))

        # Change to the directory with the vasp inputs to run custodian
        os.chdir(os.path.join(cwd,folder))

        fw_env = fw_spec.get("_fw_env", {})
        custodian_params = self.get("custodian_params", {})

        # Get the scratch directory
        if fw_env.get('scratch_root'):
            custodian_params['scratch_dir'] = os.path.expandvars(
                fw_env['scratch_root'])

        c = Custodian(gzipped_output=True, **custodian_params)

        if not debug:
            output = c.run()
            return FWAction(stored_data=output)


@explicit_serialize
class WriteAtomVaspInputs(FireTaskBase):

    """
        Writes VASP inputs for an isolated atom in a box. For calculating
        cohesive energy. Cohesive energy define as the excess energy
        of a single isolated atom (separated from the bulk) relative
        to the energy of a single atom in a bulk structure
    """

    required_params = ["atom", "folder", "cwd"]
    optional_params = ["user_incar_settings", "potcar_functional",
                       "latt_a", "kpoints", "gpu",
                       "diatomic", "diatomic_blength"]

    def run_task(self, fw_spec):

        """
            Required parameters:
                atom (str): Species of the isolated atom
                folder (str): Location to write the inputs to
                cwd (str): Current working directory
            Optional parameters:
                user_incar_settings (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                latt_a (float, Angstroms): The cubic lattice dimensions of
                    the box that the isolated atom inhabits. Defaults to
                    16 A based on convergence test with Cs
                kpoints (int): The kpoint of the box holding the isolated
                    atom. Defaults to 1 based on convergence test.
        """

        dec = MontyDecoder()
        latt_a = dec.process_decoded(self.get("latt_a", 18))
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        atom = dec.process_decoded(self.get("atom"))
        diatomic = dec.process_decoded(self.get("diatomic", False))
        diatomic_blength = dec.process_decoded(self.get("diatomic_blength", None))
        gpu = dec.process_decoded(self.get("gpu", False))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings", None))
        kpoints0 = \
            dec.process_decoded(self.get("kpoints0", 1))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_functional", 'PBE'))

        # Build the isolated atom in a box
        lattice = Lattice.cubic(latt_a)
        if diatomic:
            atom_in_a_box = Structure(lattice, [atom, atom],
                                      [[0., 0., 0.], [0, 0, diatomic_blength]],
                                      coords_are_cartesian=True)
        else:
            atom_in_a_box = Structure(lattice, [atom],
                                      [[0.5, 0.5, 0.5]])

        mplb = MVLSlabSet(atom_in_a_box, k_product=1,
                          user_incar_settings=user_incar_settings,
                          potcar_functional=potcar_functional)
        mplb.write_input(os.path.join(cwd, folder))

        incar = mplb.incar
        kpt = mplb.kpoints
        kpt.kpts[0] = [kpoints0]*3
        kpt.write_file(os.path.join(cwd, folder, "KPOINTS"))

        if gpu:
            if "KPAR" not in incar.keys():
                incar.__setitem__('KPAR', 1)
            if "NPAR" in incar.keys():
                del incar["NPAR"]
        else:
            if "KPAR" in incar.keys():
                del incar["KPAR"]

        incar.write_file(os.path.join(cwd, folder, "INCAR"))



def check_termination_symmetry(slab_list, miller_index, min_slab_size,
                               min_vacuum_size, relax_orient_uc):

    # Function to symmetrize set of slabs with different
    # terminations and prevent removal of too many atoms.

    ssize_check = False # Checks if ssize is at least that
                        # of the initial min_slab_size
    new_min_slab_size = min_slab_size
    original_num_sites = len(slab_list[0])

    new_shifts = [slab.shift for slab in slab_list]
    while ssize_check is False:

        new_slab_list = []

        slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                              min_slab_size=new_min_slab_size,
                              min_vacuum_size=min_vacuum_size,
                              max_normal_search=max(miller_index),
                              primitive=True)
        # For loop will generate a list of symmetrized
        # slabs of different terminations
        for slab in slab_list:

            # Get the symmetrize slab
            slab = slabs.symmetrize_slab(slab)

            # Just skip the calculation if false,
            # further investigation will be required...

            new_slab_list.append(slab)
            new_num_sites = len(slab)

        # Check if we still have at least 85% of the original atoms
        # in the structure after removing sites to obtain symmetry,
        # otherwise, recreate the slabs again using SlabGenerator
        # and compensate for the smaller number of sites

        if 100 * (new_num_sites/original_num_sites) < 85:
            ssize_check = False
            new_min_slab_size += 5
        else:
            ssize_check = True

        if new_min_slab_size > 20:
            warnings.warn("Too many attempts at symmetrizing/increasing "
                          "ssize, breaking out of while loop")

            slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                                  min_slab_size=min_slab_size,
                                  min_vacuum_size=min_vacuum_size,
                                  max_normal_search=max(miller_index),
                                  primitive=True)
            # Give up, new-slab_list will just contain the slabs as they are, unsymmetrized
            new_slab_list = [slabs.get_slab(shift=shift) for shift in new_shifts]

            break

        if not ssize_check:
            print("making new slabs because ssize too small")
            slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                                  min_slab_size=new_min_slab_size,
                                  min_vacuum_size=min_vacuum_size,
                                  max_normal_search=max(miller_index),
                                  primitive=True)

            new_slab_list = [slabs.get_slab(shift=shift) for shift in new_shifts]

    return new_slab_list, new_min_slab_size

class GetMillerIndices(object):

    def __init__(self, structure, max_index, reciprocal=True):

        """
        A class for obtaining a family of indices or
            unique indices up to a certain max index.

        Args:
            structure (Structure): input structure.
            max_index (int): The maximum index. For example, a max_index of 1
                means that (100), (110), and (111) are returned for the cubic
                structure. All other indices are equivalent to one of these.
        """

        recp_lattice = structure.lattice.reciprocal_lattice_crystallographic
        # Need to make sure recp lattice is big enough, otherwise symmetry
        # determination will fail. We set the overall volume to 1.
        recp_lattice = recp_lattice.scale(1)
        recp = Structure(recp_lattice, ["H"], [[0, 0, 0]])
        structure_sym = recp if reciprocal else structure

        analyzer = SpacegroupAnalyzer(structure_sym, symprec=0.001)
        symm_ops = analyzer.get_symmetry_operations()

        self.structure = structure
        self.max_index = max_index
        self.symm_ops = symm_ops

    def is_already_analyzed(self, miller_index, unique_millers=[]):

        """
        Creates a function that uses the symmetry operations in the
        structure to find Miller indices that might give repetitive orientations

        Args:
            miller_index (tuple): Algorithm will find indices
                equivalent to this index.
            unique_millers (list): Algorithm will check if the
                miller_index is equivalent to any indices in this list.
        """

        for op in self.symm_ops:
            if in_coord_list(unique_millers, op.operate(miller_index)):
                return True
        return False

    def get_symmetrically_distinct_miller_indices(self):

        """
        Returns all symmetrically distinct indices below a certain max-index for
        a given structure. Analysis is based on the symmetry of the reciprocal
        lattice of the structure.
        """

        unique_millers = []

        r = list(range(-self.max_index, self.max_index + 1))
        r.reverse()
        for miller in itertools.product(r, r, r):
            if any([i != 0 for i in miller]):
                d = abs(reduce(gcd, miller))
                miller = tuple([int(i / d) for i in miller])
                if not self.is_already_analyzed(miller, unique_millers):
                    unique_millers.append(miller)
        return unique_millers

    def get_symmetrically_equivalent_miller_indices(self, miller_index):
        """
        Returns all symmetrically equivalent indices below a certain max-index for
        a given structure. Analysis is based on the symmetry of the reciprocal
        lattice of the structure.

        Args:
            structure (Structure): input structure.
            miller_index (tuple): Designates the family of Miller indices to find.
        """
        equivalent_millers = [miller_index]
        r = list(range(-self.max_index, self.max_index + 1))
        r.reverse()

        for miller in itertools.product(r, r, r):
            # print miller
            if miller[0] == miller_index[0] and \
               miller[1] == miller_index[1] and \
               miller[2] == miller_index[2]:

                continue

            if any([i != 0 for i in miller]):
                d = abs(reduce(gcd, miller))
                miller = tuple([int(i / d) for i in miller])
                if in_coord_list(equivalent_millers, miller):
                    continue
                if self.is_already_analyzed(miller,
                                            unique_millers=equivalent_millers):
                    equivalent_millers.append(miller)

        return equivalent_millers

    def get_true_index(self, miller_index, spacegroup=None):
        if len(miller_index) == 4:
            return miller_index
        if not spacegroup:
            spacegroup = SpacegroupAnalyzer(self.structure).get_spacegroup_symbol()
        if SpaceGroup(spacegroup).crystal_system in ["trigonal",
                                                     "hexagonal"]:
            return (miller_index[0], miller_index[1],
                    -1*miller_index[0]-miller_index[1],
                    miller_index[2])
        else:
            return miller_index

    def get_reduced_index(self, miller_index):

        if len(miller_index) == 4:
            return (miller_index[0],
                    miller_index[1],
                    miller_index[3])
        else:
            return miller_index

    def hkl_str_to_tuple(self, hkl):

        # Converts a string in the format 'hkl' to (h, k, l)
        miller_index = []
        for i, index in enumerate(hkl):
            if hkl[i-1] == "-":
                miller_index.append(-1*int(index))

            elif index in ["-", ")", "(", ",", " "]:
                continue
            else:
                miller_index.append(int(index))

        return self.get_true_index(tuple(miller_index))

    def hkl_tuple_to_str(self, miller_index):

        # converts a Miller index to standard string
        # format where negative values have a bar on top.

        true_index = self.get_true_index(miller_index)
        str_format = '($'
        for x in true_index:
            if x < 0:
                str_format += '\overline{' + str(-x) +'}'
            else:
                str_format += str(x)
        str_format += '$)'

        return str_format


class SurfaceQueryEngine(QueryEngine):
    def __init__(self, db_credentials):

        super(SurfaceQueryEngine, self).__init__(**db_credentials)


"""
class summary:
    UpdateRepositoriesAndDBs(QueryEngine):
        A class used for updating the different collections in "surfacedb"
        and json files in the pymacy repo.
"""

class UpdateRepositoriesAndDBs(object):

    """
    Goes through the raw collection (surface_tasks) to
    find entries that are not preset in current
    collections or repositories


    Format of each entry in surface_properties collection:

        {"material_id": str,
         "polymorph": int,
         "weighted_surface_energy_EV_PER_ANG2": float,
         "e_above_hull": float,
         "pretty_formula": str,
         "weighted_surface_energy": float,
         "anisotropy": float,
         "spacegroup": {"symbol": str,
                        "number": int},
         "surfaces":
           [
               {"miller_index": list,
                "tasks": {"OUC": int, "slab": int},
                "surface_energy_EV_PER_ANG2": float,
                "surface_energy": float,
                "is_reconstructed": bool,
                "area_fraction": float,
                "structure": str(cif)
               },
               {
               },
               {
               },
               etc ...
           ]
        }

    Format of each entry in wulff collection:

        {"material_id": str,
         "polymorph": int,
         "pretty_formula": str,
         "spacegroup": {"symbol": str,
                        "number": int},
         "thumbnail": binary,
         "hi_res_images":
           [
               {"miller_index": list,
                "image": binary
               },
               {
               },
               {
               },
               etc ...
           ]
        }

    """

    def __init__(self, db_credentials):

        conn = MongoClient(host=db_credentials["host"],
                           port=db_credentials["port"])
        db = conn.get_database(db_credentials["database"])
        db.authenticate(db_credentials["user"],
                        db_credentials["password"])

        optional_data = ["surface_area", "nsites", "structure_type",
                         "miller_index", "polymorph", "shift", "state",
                         "material_id", "pretty_formula", "task_id",
                         "final_structure", "initial_structure",
                         "is_reconstructed", "calculations"]

        self.property_coll_to_update = db["surface_properties"]
        self.vasp_details = db["vasp_details"]
        self.surface_tasks = db["surface_tasks"]
        self.wulff_coll_to_update = db["wulff"]
        self.optional_data = optional_data
        self.mprester = MPRester(apikey)
        self.qe = SurfaceQueryEngine(db_credentials)

    def insert_surface_property_entry(self, mpid):

        """
        Sets up the basic metadata information for an entry, i.e.
            e_above_hull, material_id, pretty_formula etc.

            Args:
                mpid (str):
                    material id from MP for a material
        """

        entry = {}

        task_entry = self.surface_tasks.find_one({"material_id": mpid,
                                                  "structure_type": "slab_cell"})
        if not task_entry:
            warnings.warn("No calculations completed for this material.")
            return

        mp_entry = self.mprester.get_entries(mpid,
                                             property_data=["spacegroup",
                                                            "e_above_hull"])[0]

        if mpid not in self.property_coll_to_update.distinct("material_id"):

            sp_symbol = task_entry["conventional_spacegroup"]
            entry["material_id"] = mpid
            entry["polymorph"] = task_entry["polymorph"]

            entry["spacegroup"] = {"symbol": sp_symbol,
                                   "number": SpaceGroup(sp_symbol).int_number}
            entry["pretty_formula"] = task_entry["pretty_formula"]
            entry["e_above_hull"] = mp_entry.data["e_above_hull"]

            self.property_coll_to_update.insert(entry)

        if mpid not in self.vasp_details.distinct("material_id"):
            self.vasp_details.insert({"material_id": mpid})

        self.update_surface_property_entry(mpid)

    def update_surface_property_entry(self, mpid):

        """
        Query raw information from the task collection for post processing
            and insertion into the surface_properties database.

            Args:
                mpid (str):
                    material id from MP for a material
        """

        if mpid not in self.property_coll_to_update.distinct("material_id"):
            warnings.warn("No entry in surface_properties collection, update "
                          "collection with insert_surface_property_entry()")
            return

        miller_handler = GetMillerIndices(get_conventional_ucell(mpid, from_mapi=False,
                                                                 qe=self.qe), 3)
        ucell_entries = self.qe.get_entries({"structure_type": "oriented_unit_cell",
                                             "material_id": mpid}, inc_structure="Final",
                                            optional_data=self.optional_data)

        entries_dict = self.get_entries_dict(ucell_entries)
        # Make updates to individual surfaces
        e_surf_list, miller_list, surfaces, slab_vasp_details, \
        ucell_energies = [], [], [], [], []
        for hkl in entries_dict.keys():
            # For loop will create a list of surface dictionaries
            # containing information on specfic surfaces

            if not entries_dict[hkl]["slabcell"]:
                continue
            miller_index = miller_handler.get_true_index(hkl)
            surface = {"miller_index": miller_index}

            miller_list.append(miller_index)
            tasks = {"OUC": entries_dict[hkl]["ucell"].data["task_id"]}

            surface_energies = []
            for slab_entry in entries_dict[hkl]["slabcell"]:
                surface_energy = ((slab_entry.energy - slab_entry.data["nsites"]*
                                   entries_dict[hkl]["ucell"].energy_per_atom)/
                                  (2*slab_entry.data["surface_area"]))*\
                                 EV_PER_ANG2_TO_JOULES_PER_M2
                surface_energies.append(surface_energy)

            # sort by surface energy, assume first entry is the
            # most stable termination, if a system reconstructs,
            # assume the first entry is a reconstruction and the
            # second entry is the most stable ideal surface/termination
            surface_energies, entries_dict[hkl]["slabcell"] = \
                zip(*sorted(zip(surface_energies,
                                entries_dict[hkl]["slabcell"])))

            count = 0
            true_slab_entry = entries_dict[hkl]["slabcell"][count]

            e_surf_list.append(surface_energies[count])
            tasks["slab"] = true_slab_entry.data["task_id"]
            surface["tasks"] = tasks.copy()
            surface["surface_energy_EV_PER_ANG2"] = surface_energies[count]/EV_PER_ANG2_TO_JOULES_PER_M2
            surface["surface_energy"] = surface_energies[count]
            surface["is_reconstructed"] = True if true_slab_entry.data["is_reconstructed"] else False
            surface["structure"] = true_slab_entry.structure.to("cif")
            surface["initial_structure"] = \
                Structure.from_dict(true_slab_entry.data["initial_structure"]).to("cif")

            surfaces.append(surface)

            # Create a dictionary for the details on VASP calculations
            surface_vasp_detail = {"miller_index": miller_index}
            surface_vasp_detail["is_reconstructed"] = True if true_slab_entry.data["is_reconstructed"] else False
            surface_vasp_detail["calculations"] =  {}
            relax1 = true_slab_entry.data["calculations"][0]

            for key in relax1.keys():
                if key in ["input", "output"]:
                    continue
                surface_vasp_detail["calculations"][key] = relax1[key]
            surface_vasp_detail["calculations"]["input"] = relax1["input"]
            if len(true_slab_entry.data["calculations"]) > 1:
                output = true_slab_entry.data["calculations"][1]["output"]
            else:
                output = true_slab_entry.data["calculations"][0]["output"]

            new_output = {}
            for key in output.keys():
                if key in ["epsilon_static", "epsilon_static_wolfe", "cbm", "vbm",
                           "bandgap", "epsilon_ionic", "efermi", "is_gap_direct",
                           "eigenvalues"]:
                    continue
                else:
                    new_output[key] = output[key]

            surface_vasp_detail["calculations"]["output"] = new_output

            slab_vasp_details.append(surface_vasp_detail)

        if len(e_surf_list) < 1:
            warnings.warn("Require at least one surface before inserting, skipping %s" %(mpid))
            return
        elif len(e_surf_list) < 2:
            wulff_shape = None
        else:
            try:
                wulff_shape = self.get_wulff(mpid, miller_list=miller_list,
                                        e_surf_list=e_surf_list)
            except RuntimeError:
                wulff_shape = None

        weighted_energy = {"weighted_surface_energy": wulff_shape.weighted_surface_energy,
                           "weighted_surface_energy_EV_PER_ANG2": wulff_shape.weighted_surface_energy/\
                                                                  EV_PER_ANG2_TO_JOULES_PER_M2} \
            if wulff_shape else {"weighted_surface_energy": e_surf_list[0],
                                 "weighted_surface_energy_EV_PER_ANG2": e_surf_list[0]/\
                                                                        EV_PER_ANG2_TO_JOULES_PER_M2}

        area_fraction = wulff_shape.area_fraction_dict if wulff_shape else {tuple(miller_list[0]): 1}

        self.property_coll_to_update.update_one({"material_id": mpid},
                                                {"$set": weighted_energy})

        if wulff_shape:
            for surface in surfaces:
                surface["area_fraction"] = area_fraction[surface["miller_index"]]

        anisotropy = wulff_shape.anisotropy if wulff_shape else 0
        shape_factor = wulff_shape.shape_factor if wulff_shape else None

        self.property_coll_to_update.update_one({"material_id": mpid},
                                   {"$set": {"surfaces": surfaces,
                                             "surface_anisotropy": anisotropy,
                                             "shape_factor": shape_factor}})

        self.vasp_details.update_one({"material_id": mpid},
                                     {"$set": {"surfaces": slab_vasp_details}})

    def get_entries_dict(self, ucell_entries, mpid):

        entries_dict = {}
        for entry in ucell_entries:
            entries_dict[tuple(entry.data["miller_index"])] = \
                {"ucell": entry,
                 "slabcell": self.qe.get_entries({"structure_type": "slab_cell",
                                                  "material_id": mpid,
                                                  "miller_index": tuple(entry.data["miller_index"])},
                                                 inc_structure="Final",
                                                 optional_data=self.optional_data)}

    def insert_wulff_entry(self, mpid):

        """
        Inserts the Wulff shape image of the materials in
        this SlabDict object into a Wulff database.

            Args:
                mpid (str):
                    material id from MP for a material
        """

        if not self.wulff_coll_to_update:
            raise EntryError("No collection specified for parameter: wulff_collection")

        if mpid not in self.property_coll_to_update.distinct("material_id"):
            warnings.warn("No entry in surface_properties collection, update "
                          "collection with insert_surface_property_entry()")
            return

        else:
            material_props = self.property_coll_to_update.find_one({"material_id": mpid})

        if mpid not in self.wulff_coll_to_update.distinct("material_id"):
            entry = {"material_id": mpid}
            entry["polymorph"] = material_props["polymorph"]
            entry["spacegroup"] = material_props["spacegroup"]
            entry["pretty_formula"] = material_props["pretty_formula"]
            self.wulff_coll_to_update.insert(entry)

        self.update_wulff_entry(mpid)

    def update_wulff_entry(self, mpid):

        # Function used by insert_wulff_shapes() to
        # insert the image of the Wulff shape in the DB

        if mpid not in self.wulff_coll_to_update.distinct("material_id"):
            warnings.warn("No entry in wulff collection, update "
                          "collection with insert_wulff_entry()")
            return

        miller_list = [tuple(surface["miller_index"]) for surface in \
                       self.property_coll_to_update.find_one({"material_id": mpid})["surfaces"]]

        ucell = get_conventional_ucell(mpid, from_mapi=False, qe=self.qe)

        # Get all miller indices up to 2 and insert the
        # wulff images in those directions initially
        miller_handler = GetMillerIndices(ucell, 2)
        mill_list_max2 = miller_handler.get_symmetrically_distinct_miller_indices()

        for hkl in mill_list_max2:
            mill = miller_handler.get_true_index(hkl)
            if mill not in miller_list:
                miller_list.append(mill)
        print(miller_list, len(miller_list))
        wulff = self.get_wulff(mpid)

        wulff_plot = wulff.get_plot(bar_on=False, legend_on=False)
        data = cStringIO.StringIO()

        wulff_plot.savefig(data, transparent=True, dpi=30,
                           bbox_inches='tight', pad_inches=-1.25)
        wulff_plot.close()
        self.wulff_coll_to_update.update_one({"material_id": mpid},
                                {"$set": {"thumbnail": binary.Binary(data.getvalue())}})

        new_images = []
        for miller_index in miller_list:
            image = {}

            wulff_plot = wulff.get_plot(direction=miller_index,
                                        bar_on=False,
                                        legend_on=True)
            data = cStringIO.StringIO()
            wulff_plot.savefig(data, transparent=True, dpi=100)
            wulff_plot.close()
            image["miller_index"] = miller_index
            image["image"] = binary.Binary(data.getvalue())
            new_images.append(image)

        self.wulff_coll_to_update.update_one({"material_id": mpid},
                                {"$set": {"hi_res_images": new_images}})

    def get_wulff(self, mpid, miller_list=[], e_surf_list=[]):

        ucell = get_conventional_ucell(mpid, from_mapi=False, qe=self.qe)
        if not (miller_list or e_surf_list):
            miller_list = []
            e_surf_list = []

            entry = self.property_coll_to_update.find_one({"material_id": mpid})

            for surface in entry["surfaces"]:
                miller_list.append(surface["miller_index"])
                e_surf_list.append(surface["surface_energy"])

        return WulffShape(ucell.lattice, miller_list, e_surf_list)


def get_conventional_ucell(formula_id, symprec=1e-3,
                           angle_tolerance=5, from_mapi=True,
                           qe=None):

    """
    Gets the conventional unit cell by querying
    materials project for the primitive unit cell

    Args:
        formula_id (string): Materials Project ID
            associated with the slab data entry.
    """

    entries = mprester.get_entries(formula_id, inc_structure="Final",
                                   property_data=["e_above_hull"])
    if formula_id[:2] == "mp":
        prim_unit_cell = entries[0].structure
    else:
        ehulls = [entry.data["e_above_hull"] for entry in entries]
        ehulls, entries = zip(*sorted(zip(ehulls, entries)))
        prim_unit_cell = entries[0].structure

    spa = SpacegroupAnalyzer(prim_unit_cell, symprec=symprec,
                             angle_tolerance=angle_tolerance)
    ucell = spa.get_conventional_standard_structure()

    if from_mapi:
        return ucell
    else:
        miller_handler = GetMillerIndices(ucell, 1)
        criteria = {"structure_type": "oriented_unit_cell",
                    "material_id": formula_id}

        for hkl in miller_handler.get_symmetrically_equivalent_miller_indices((0,0,1)):
            criteria["miller_index"] = tuple(hkl)
            conv_ucell_entries = qe.get_entries(criteria, inc_structure="Final")
            if conv_ucell_entries:
                return conv_ucell_entries[0].structure
            else:
                return False
