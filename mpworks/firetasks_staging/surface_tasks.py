## for Surface Energy Calculation
from __future__ import division, unicode_literals

__author__ = "Zihan XU"
__version__ = "0.1"
__email__ = "vivid0036@gmail.com"
__date__ = "6/2/15"

import os
import numpy as np
import warnings

from fireworks.core.firework import FireTaskBase, FWAction, Firework
from fireworks import explicit_serialize

from custodian.custodian import Custodian
from custodian.vasp.jobs import VaspJob

from matgendb.creator import VaspToDbTaskDrone
from matgendb import QueryEngine

from pymatgen.io.vaspio_metal_slabs import MPSlabVaspInputSet
from pymatgen.io.vasp.outputs import Incar, Outcar, Poscar, Oszicar
from pymatgen.core.surface import SlabGenerator, GetMillerIndices, symmetrize_slab
from pymatgen.core.structure import Structure, Lattice
from pymatgen.matproj.rest import MPRester
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.analysis.structure_analyzer import RelaxationAnalyzer, VoronoiConnectivity


from monty.json import MontyDecoder


"""
Firework tasks
"""


@explicit_serialize
class WarningAnalysisTask(FireTaskBase):

    """
        Checks for red flags in the slab calculations, creates a list of
        warnings (strings) and inserts it into a VaspSlabDBInsertTask
    """

    required_params = ["host", "port", "user", "password",
                       "database", "collection", "mpid",
                       "struct_type", "loc","miller_index", "vaspdbinsert_parameters",
                       "cwd", "conventional_spacegroup", "polymorph", "conventional_unit_cell"]
    optional_params = ["surface_area", "shift", "vsize", "ssize", "isolated_atom"]

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

        dec = MontyDecoder()
        struct_type = dec.process_decoded(self.get("struct_type"))
        loc = dec.process_decoded(self.get("loc"))
        cwd = dec.process_decoded(self.get("cwd"))
        surface_area = dec.process_decoded(self.get("surface_area", None))
        shift = dec.process_decoded(self.get("shift", None))
        vsize = dec.process_decoded(self.get("vsize", None))
        ssize = dec.process_decoded(self.get("ssize", None))
        miller_index = dec.process_decoded(self.get("miller_index"))
        mpid = dec.process_decoded(self.get("mpid"))
        polymorph = dec.process_decoded(self.get("polymorph"))
        spacegroup = dec.process_decoded(self.get("conventional_spacegroup"))
        isolated_atom = dec.process_decoded(self.get("isolated_atom", None))
        conventional_unit_cell = dec.process_decoded(self.get("conventional_unit_cell"))
        vaspdbinsert_parameters = dec.process_decoded(self.get("vaspdbinsert_parameters"))

        # Sets default for DB parameters
        if not self["host"]:
            self["host"] = "127.0.0.1"
        if not self["port"]:
            self["port"] = 27017
        if not self["database"]:
            self["database"] = "vasp"
        if not self["collection"]:
            self["collection"] = "tasks"

        initial = Poscar.from_file(cwd+loc+'/POSCAR')
        final = Poscar.from_file(cwd+loc+'/CONTCAR.relax2.gz')

        warnings = []

        # Check if the spacegroup queried from MP API consistent
        # with the one calculated from the queried structure
        queried_sg = spacegroup
        spa = SpacegroupAnalyzer(conventional_unit_cell,
                                 symprec=0.001, angle_tolerance=5)
        calculated_sg = spa.get_spacegroup_symbol()
        if str(calculated_sg) != queried_sg:
            warnings.append("api_mp_spacegroup_inconsistent")

        qe = QueryEngine(**vaspdbinsert_parameters)
        ucell_entry = qe.get_entries({'material_id': mpid, 'miller_index': miller_index,
                                      'structure_type': 'oriented_unit_cell'})[0]

        init_bulk = Structure.from_dict(ucell_entry.data["initial_structure"])
        fin_bulk = Structure.from_dict(ucell_entry.data["final_structure"])

        # Analyze bulk relaxations for possible
        # warning signs, too much volume relaxation
        relaxation = RelaxationAnalyzer(init_bulk, fin_bulk)
        if abs(relaxation.get_percentage_volume_change()*100) > 1:
            warnings.append("|bulk_vol_rel|>1%")

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

        # For Check negative surface energy
        e_per_atom = ucell_entry.energy_per_atom
        # Find out if an entry for this slab already exists.
        # If so, see if the slab at shift=0 has been calculated,
        # then calculate all other terminations besides c=0
        final_energy = Oszicar(cwd+loc+'/OSZICAR.relax2.gz').final_energy
        surface_e = final_energy - e_per_atom*len(initial)
        if surface_e < 0:
            warnings.append("negative_surface_energy")

        fw = Firework([VaspSlabDBInsertTask(struct_type="slab_cell",
                                            loc=loc, cwd=cwd, shift=shift,
                                            surface_area=surface_area,
                                            vsize=vsize,
                                            ssize=ssize,
                                            miller_index=miller_index,
                                            mpid=mpid, polymorph=polymorph,
                                            conventional_spacegroup=spacegroup,
                                            warnings=warnings, **vaspdbinsert_parameters)])

        FW = [fw]
        return FWAction(additions=FW)

@explicit_serialize
class VaspSlabDBInsertTask(FireTaskBase):

    """
        Inserts a single vasp calculation in a folder into a
        DB. Also inserts useful information pertaining
        to slabs and oriented unit cells.
    """

    required_params = ["host", "port", "user", "password",
                       "database", "collection", "mpid",
                       "struct_type", "loc","miller_index",
                       "cwd", "conventional_spacegroup", "polymorph"]
    optional_params = ["warnings", "surface_area", "shift",
                       "vsize", "ssize", "isolated_atom"]

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

        dec = MontyDecoder()
        struct_type = dec.process_decoded(self.get("struct_type"))
        loc = dec.process_decoded(self.get("loc"))
        cwd = dec.process_decoded(self.get("cwd"))
        surface_area = dec.process_decoded(self.get("surface_area", None))
        shift = dec.process_decoded(self.get("shift", None))
        vsize = dec.process_decoded(self.get("vsize", None))
        ssize = dec.process_decoded(self.get("ssize", None))
        miller_index = dec.process_decoded(self.get("miller_index"))
        mpid = dec.process_decoded(self.get("mpid"))
        polymorph = dec.process_decoded(self.get("polymorph"))
        spacegroup = dec.process_decoded(self.get("conventional_spacegroup"))
        isolated_atom = dec.process_decoded(self.get("isolated_atom", None))
        warnings = dec.process_decoded(self.get("warnings", None))

        # Sets default for DB parameters
        if not self["host"]:
            self["host"] = "127.0.0.1"
        if not self["port"]:
            self["port"] = 27017
        if not self["database"]:
            self["database"] = "vasp"
        if not self["collection"]:
            self["collection"] = "tasks"

        name = loc.replace("/", "")

        # Addtional info relating to slabs
        additional_fields = {
                             "author": os.environ.get("USER"),
                             # User that ran the calculation
                             "structure_type": struct_type,
                             "miller_index": miller_index,
                             "surface_area": surface_area, "shift": shift,
                             "vac_size": vsize, "slab_size": ssize,
                             "material_id": mpid, "conventional_spacegroup": spacegroup,
                             "isolated_atom": isolated_atom,
                             "polymorph": polymorph,
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

        drone = VaspToDbTaskDrone(host=self["host"], port=self["port"],
                                  user=self["user"],
                                  password=self["password"],
                                  database=self["database"],
                                  use_full_uri=False,
                                  additional_fields=additional_fields,
                                  collection=self["collection"])
        drone.assimilate(cwd+loc)


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
                       "latt_a", "kpoints", ]

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
        latt_a = dec.process_decoded(self.get("latt_a", 16))
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        atom = dec.process_decoded(self.get("atom"))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings",
                                         MPSlabVaspInputSet().incar_settings))
        kpoints0 = \
            dec.process_decoded(self.get("kpoints0", 1))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_fuctional", 'PBE'))


        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  kpoints0=[kpoints0]*3, bulk=False,
                                  potcar_functional=potcar_functional,
                                  ediff_per_atom=False)

        # Build the isolated atom in a box
        lattice = Lattice.cubic(latt_a)
        atom_in_a_box = Structure(lattice, [atom], [[0.5, 0.5, 0.5]])

        mplb.write_input(atom_in_a_box, cwd+folder)


@explicit_serialize
class WriteUCVaspInputs(FireTaskBase):
    """
        Writes VASP inputs for an oriented unit cell
    """

    required_params = ["oriented_ucell", "folder", "cwd"]
    optional_params = ["user_incar_settings",
                       "k_product", "potcar_functional"]

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
                k_product (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
        """
        dec = MontyDecoder()
        oriented_ucell = dec.process_decoded(self.get("oriented_ucell"))
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings",
                                         MPSlabVaspInputSet().incar_settings))
        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_fuctional", 'PBE'))


        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  k_product=k_product, bulk=True,
                                  potcar_functional=potcar_functional,
                                  ediff_per_atom=False)
        mplb.write_input(oriented_ucell, cwd+folder)


@explicit_serialize
class WriteSlabVaspInputs(FireTaskBase):
    """
        Adds dynamicism to the workflow by creating addition Fireworks for each
        termination of a slab or just one slab with shift=0. First the vasp
        inputs of a slab is created, then the Firework for that specific slab
        is made with a RunCustodianTask and a VaspSlabDBInsertTask
    """
    required_params = ["folder", "cwd", "custodian_params",
                       "vaspdbinsert_parameters", "miller_index",
                       "mpid", "conventional_spacegroup", "polymorph"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "user_incar_settings",
                       "k_product","potcar_functional"]

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
                k_product (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
        """
        dec = MontyDecoder()
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        custodian_params = dec.process_decoded(self.get("custodian_params"))
        vaspdbinsert_parameters = \
            dec.process_decoded(self.get("vaspdbinsert_parameters"))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings",
                                         MPSlabVaspInputSet().incar_settings))
        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_fuctional", 'PBE'))
        min_slab_size = dec.process_decoded(self.get("min_slab_size", 10))
        min_vacuum_size = dec.process_decoded(self.get("min_vacuum_size", 10))
        miller_index = dec.process_decoded(self.get("miller_index"))
        mpid = dec.process_decoded(self.get("mpid"))
        polymorph = dec.process_decoded(self.get("polymorph"))
        spacegroup = dec.process_decoded(self.get("conventional_spacegroup"))

        #is_primitive = True

        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  k_product=k_product,
                                  potcar_functional=potcar_functional,
                                  ediff_per_atom=False)

        # Create slabs from the relaxed oriented unit cell. Since the unit
        # cell is already oriented with the miller index, entering (0,0,1)
        # into SlabGenerator is the same as obtaining a slab in the
        # orienetation of the original miller index.

        qe = QueryEngine(**vaspdbinsert_parameters)
        optional_data = ["state", "shift", "final_incar", "final_magnetization"]
        ucell_entry = qe.get_entries({'material_id': mpid, 'structure_type': 'oriented_unit_cell',
                                'miller_index': miller_index}, inc_structure=True,
                               optional_data=optional_data)[0]

        relax_orient_uc = ucell_entry.structure

        slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                              min_slab_size=min_slab_size,
                              min_vacuum_size=min_vacuum_size,
                              max_normal_search=max(miller_index),
                              primitive=True)

        # Find out if an entry for this slab already exists.
        # If so, see if the slab at shift=0 has been calculated,
        # then calculate all other terminations besides c=0

        slab_entries = qe.get_entries({'material_id': mpid, 'structure_type': 'slab_cell',
                                       'miller_index': miller_index}, inc_structure=True,
                                      optional_data=optional_data)
        slab_list = slabs.get_slabs()
        new_term_shifts = [slab.shift for slab in slab_list]

        if len(slab_entries) > 0 and len(slab_entries) != len(slab_list):

            # Check if any and if all terminations for this slab have been
            # completed. If a termination has been calculated, check
            # which one and skip the calculation for that termination.

            completed_term_shifts = [entry.data["shift"] for entry in slab_entries]

            if 0 in completed_term_shifts:

                # Because the default shift use to be 0, remove the
                # lowest shift in the new list of shifts for different
                # terminations to avoid repeated calculations

                slab_list.pop(new_term_shifts.index(min(new_term_shifts)))
                new_term_shifts.pop(new_term_shifts.index(min(new_term_shifts)))
                new_term_shifts.pop(new_term_shifts.index(min(new_term_shifts)))
                completed_term_shifts.pop(completed_term_shifts.index(0))

            if completed_term_shifts:

                for shift in completed_term_shifts:

                    # Now remove calculations that have already
                    # been completed for a particular termination

                    slab_list.pop(new_term_shifts.index(shift))

        print 'chemical formula', relax_orient_uc.composition.reduced_formula
        print 'mpid', mpid
        print "Miller Index: ", miller_index
        print ucell_entry.data['state']

        # Check if ucell calculation was successful before doing slab calculation
        if ucell_entry.data['state'] != 'successful':
            print "%s bulk calculations were incomplete, cancelling FW" \
                  %(relax_orient_uc.composition.reduced_formula)
            return FWAction()
        else:
            print ucell_entry.data['state']

            FWs = []

            # Make a slab from scratch to double check
            # if we're using the most reduced structure

            if "MAPI_KEY" not in os.environ:
                apikey = raw_input('Enter your api key (str): ')
            else:
                apikey = os.environ["MAPI_KEY"]
            mprester = MPRester(apikey)
            prim_unit_cell = mprester.get_entries(mpid,
                                                  inc_structure=True)[0].structure
            spa = SpacegroupAnalyzer(prim_unit_cell)
            conventional_ucell = spa.get_conventional_standard_structure()

            slabs = SlabGenerator(conventional_ucell, miller_index,
                                  min_slab_size=min_slab_size,
                                  min_vacuum_size=min_vacuum_size,
                                  max_normal_search=max(miller_index),
                                  primitive=True)
            # prim_sites = len(slabs.get_slab())

            for slab in slab_list:

                # # Check if the slab is reduced
                # if len(slab) > prim_sites:
                #     warnings.warn("This slab has way too many atoms in it, will attempt "
                #                   "to construct slab from relaxed conventional ucell")
                #
                #     shift = slab.shift
                #
                #     # If the slab constructed from the oriented ucell is not reduced,
                #     # attempt to make a reduced slab from the relaxed conventional ucell
                #
                #     rel_ucell = None
                #     getmillerindices = GetMillerIndices(conventional_ucell,1)
                #     eq_mills = getmillerindices.get_symmetrically_equivalent_miller_indices((0,0,1))
                #     conventional_ucell_calculated = False
                #
                #     # Need to find the miller index of an
                #     # existing calculated conventional ucell first
                #
                #     for c_index in eq_mills:
                #         entry = qe.get_entries({'material_id': mpid,
                #                                 'structure_type': 'oriented_unit_cell',
                #                                 'miller_index': c_index},
                #                                 inc_structure=True,
                #                                 optional_data=optional_data)
                #         if entry:
                #             rel_ucell = entry[0].structure
                #
                #         if rel_ucell:
                #             slabs = SlabGenerator(rel_ucell, miller_index,
                #                                   min_slab_size=min_slab_size,
                #                                   min_vacuum_size=min_vacuum_size,
                #                                   max_normal_search=max(miller_index),
                #                                   primitive=True)
                #             slab = slabs.get_slab(shift=shift)
                #             conventional_ucell_calculated = True
                #
                #     # If slab is still not reduced, do not add the additional FW
                #
                #     if len(slab) > prim_sites:
                #         if not conventional_ucell_calculated:
                #             warnings.warn("The relaxed conventional unit cell has "
                #                           "not been calculated yet, cannot attempt "
                #                           "to create reduced structure")
                #         if conventional_ucell_calculated:
                #             warnings.warn("The relaxed conventional unit cell has "
                #                           "been calculated. We are still unable to "
                #                           "create the reduced structure, skipping...")
                #         print slab
                #         is_primitive = False

                # Now check if the two surfaces in the slab are equivalent
                is_symmetric = False
                sg = SpacegroupAnalyzer(slab, symprec=1E-3)
                pg = sg.get_point_group()
                Laue_groups = ["-1", "2/m", "mmm", "4/m", "4/mmm", "-3",
                               "-3m", "6/m", "6/mmm", "m-3", "m-3m"]
                if str(pg) in Laue_groups:
                    is_symmetric = True
                else:
                    slab = symmetrize_slab(slab)
                    sg = SpacegroupAnalyzer(slab, symprec=1E-3)
                    pg = sg.get_point_group()
                    if str(pg) in Laue_groups:
                        is_symmetric = True
                        # Just skip the calculation if false,
                        # further investigation will be required...

                new_folder = folder.replace('bulk', 'slab')+'_shift%s' \
                                                            %(slab.shift)

                mplb.write_input(slab, cwd+new_folder)

                # Inherit the final magnetization of a slab
                # from the outcar of the ucell calculation.

                out_mag = ucell_entry.data["final_magnetization"]
                if not out_mag or out_mag[0]['tot'] <= 0:
                    warnings.warn("Magnetization not found in OUTCAR.relax2.gz, "
                                  "may be incomplete, will set default magmom")
                    if slab.composition.reduced_formula in ["Fe", "Co", "Ni"]:
                        out_mag = [{'tot': 5}]
                    else:
                        out_mag = [{'tot': 0.6}]
                if out_mag[0]['tot'] == 0:
                    warnings.warn("Magnetization is 0, "
                                  "changing magnetization to non-zero")
                    out_mag = [{'tot': 1E-5}]

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
                                       optional_data=optional_data)[0]
                incar = slab_entry.data["final_incar"] if slab_entry else ucell_entry.data["final_incar"]
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
                if "NBANDS" in incar.keys():
                    incar.pop("NBANDS")
                incar.write_file(cwd+new_folder+'/INCAR')

                fw = Firework([RunCustodianTask(dir=new_folder, cwd=cwd,
                                                **custodian_params),
                               WarningAnalysisTask(struct_type="slab_cell",
                                                   loc=new_folder, cwd=cwd, shift=slab.shift,
                                                   surface_area=slab.surface_area,
                                                   vsize=slabs.min_vac_size,
                                                   ssize=slabs.min_slab_size,
                                                   miller_index=miller_index,
                                                   mpid=mpid, conventional_spacegroup=spacegroup,
                                                   polymorph=polymorph,
                                                   conventional_unit_cell=conventional_ucell,
                                                   vaspdbinsert_parameters=vaspdbinsert_parameters)],
                              name=new_folder)

                FWs.append(fw)

            # Skip this calculation if the surfaces aren't symmetric
            if is_symmetric:
                return FWAction(additions=FWs)


@explicit_serialize
class RunCustodianTask(FireTaskBase):
    """
        Runs Custodian.
    """

    required_params = ["dir", "jobs", "cwd"]
    optional_params = ["custodian_params", "handlers", "max_errors"]

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
        dir = dec.process_decoded(self['dir'])
        cwd = dec.process_decoded(self['cwd'])

        # Change to the directory with the vasp inputs to run custodian
        os.chdir(cwd+dir)

        handlers = dec.process_decoded(self.get('handlers', []))
        jobs = dec.process_decoded(self['jobs'])
        max_errors = dec.process_decoded(self['max_errors'])
        fw_env = fw_spec.get("_fw_env", {})
        cust_params = self.get("custodian_params", {})

        # Get the scratch directory
        if fw_env.get('scratch_root'):
            cust_params['scratch_dir'] = os.path.expandvars(
                fw_env['scratch_root'])

        c = Custodian(handlers=handlers, jobs=jobs, max_errors=max_errors,
                      gzipped_output=True, **cust_params)
        output = c.run()

        return FWAction(stored_data=output)


@explicit_serialize
class MoveDirectoryTask(FireTaskBase):
    """
    Basic task to create new directories to move
    and organize completed directories. This will
    prevent the home directory from being filled up.
    """

    required_params = ["cwd", "formula", "miller_index",
                       "mpid", "final_directory"]

    def run_task(self, fw_spec):

        """

        """

        dec = MontyDecoder()
        final_directory = dec.process_decoded(self['final_directory'])
        cwd = dec.process_decoded(self['cwd'])
        os.chdir(cwd)
        miller_index = dec.process_decoded(self['miller_index'])
        mpid = dec.process_decoded(self['mpid'])
        formula = dec.process_decoded(self['formula'])

        subdir = formula + "_" + mpid

        final_subdirs = [d for d in os.listdir(final_directory) if os.path.isdir(d)]

        if subdir not in final_subdirs:
            os.system('mkdir %s' %(final_directory + '/' + subdir))

        directories = [d for d in os.listdir(cwd) if os.path.isdir(d)]

        hkl = str(miller_index[0]) + str(miller_index[1]) + str(miller_index[2])
        for directory in directories:
            if hkl in directory and mpid in directory:
                os.system('mv %s %s' %(directory, final_directory + '/' + subdir))