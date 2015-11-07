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
from pymatgen.io.vasp.outputs import Incar, Outcar
from pymatgen.core.surface import SlabGenerator
from pymatgen.core.structure import Structure, Lattice

from monty.json import MontyDecoder


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

    required_params = ["host", "port", "user", "password",
                       "database", "collection", "struct_type", "loc",
                       "cwd", "miller_index", "mpid", "conventional_spacegroup"]
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
                struct_type (str): either oriented_unit_cell or slab_cell
                miller_index (list): Miller Index of the oriented
                    unit cell or slab
                loc (str path): Location of the outputs of
                    the vasp calculations
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
        spacegroup = dec.process_decoded(self.get("conventional_spacegroup"))
        isolated_atom = dec.process_decoded(self.get("isolated_atom", None))

        # Sets default for DB parameters
        if not self["host"]:
            self["host"] = "127.0.0.1"

        if not self["port"]:
            self["port"] = 27017

        if not self["database"]:
            self["database"] = "vasp"

        if not self["collection"]:
            self["collection"] = "tasks"

        warnings = []
        warnings.append("SE<0")
        warnings.append("%REL>%s")

        # Addtional info relating to slabs
        additional_fields = {"author": os.environ.get("USER"),
                             "structure_type": struct_type,
                             "miller_index": miller_index,
                             "surface_area": surface_area, "shift": shift,
                             "vac_size": vsize, "slab_size": ssize,
                             "material_id": mpid, "conventional_spacegroup": spacegroup,
                             "warnings": warnings,
                             "isolated_atom": isolated_atom}

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
        Writes VASP inputs for an oriented unit cell
    """

    required_params = ["atom", "folder", "cwd"]
    optional_params = ["user_incar_settings", "potcar_functional",
                       "latt_a", "kpoints", ]

    def run_task(self, fw_spec):

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
            Optional Parameters:
                angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
                user_incar_settings (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                k_product (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                symprec (float): See SpaceGroupAnalyzer in analyzer.py
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
                       "mpid", "conventional_spacegroup"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "user_incar_settings",
                       "k_product","potcar_functional",
                       "terminations"]

    def run_task(self, fw_spec):

        """
            Required Parameters:
                folder (str path): Location where vasp inputs
                    are to be written
                custodian_params (dict **kwargs): Contains the job and the
                    scratch directory for a custodian run
                vaspdbinsert_parameters (dict **kwargs): Contains
                    informations needed to acess a DB, eg, host,
                    port, password etc.
            Optional Parameters:
                min_vac_size (float): Size of vacuum layer of slab in Angstroms
                min_slab_size (float): Size of slab layer of slab in Angstroms
                angle_tolerance (int): See SpaceGroupAnalyzer in analyzer.py
                user_incar_settings (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                k_product (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                potcar_functional (dict): See launch_workflow() method in
                    CreateSurfaceWorkflow class
                symprec (float): See SpaceGroupAnalyzer in analyzer.py
                terminations (bool): Determines whether or not to consider
                    different terminations in a slab. If true, each slab with a
                    specific shift value will have its own Firework and each of the
                    slab calculations will run in parallel. Defaults to false which
                    sets the shift value to 0.
        """
        dec = MontyDecoder()
        folder = dec.process_decoded(self.get("folder"))
        cwd = dec.process_decoded(self.get("cwd"))
        terminations = dec.process_decoded(self.get("terminations", False))
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
        spacegroup = dec.process_decoded(self.get("conventional_spacegroup"))

        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  k_product=k_product,
                                  potcar_functional=potcar_functional,
                                  ediff_per_atom=False)

        # Create slabs from the relaxed oriented unit cell. Since the unit
        # cell is already oriented with the miller index, entering (0,0,1)
        # into SlabGenerator is the same as obtaining a slab in the
        # orienetation of the original miller index.

        qe = QueryEngine(**vaspdbinsert_parameters)
        optional_data = ["state"]
        entry = qe.get_entries({'material_id': mpid, 'structure_type': 'oriented_unit_cell',
                                'miller_index': miller_index}, inc_structure=True,
                               optional_data=optional_data)[0]

        relax_orient_uc = entry.structure

        # print relax_orient_uc

        slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                              min_slab_size=min_slab_size,
                              min_vacuum_size=min_vacuum_size,
                              max_normal_search=max(miller_index),
                              primitive=True)

        print "primitive is set to ", slabs.primitive

        # Whether or not to create a list of Fireworks
        # based on different slab terminations
        print 'deciding terminations'
        slab_list = slabs.get_slabs() if terminations else [slabs.get_slab()]

        print 'chemical formula', relax_orient_uc.composition.reduced_formula
        print 'mpid', mpid
        print "Miller Index: ", miller_index

        print entry.data['state']
        if entry.data['state'] != 'successful':
            print "%s bulk calculations were incomplete, cancelling FW" \
                  %(relax_orient_uc.composition.reduced_formula)
            return FWAction()
        else:

            print entry.data['state']

            FWs = []
            for slab in slab_list:
                if len(slab) >= 20:
                    warnings.warn("This slab has way too many atoms in it, "
                                  "are you sure it is the most reduced structure?")
                    print slab

                new_folder = folder.replace('bulk', 'slab')+'_shift%s' \
                                                            %(slab.shift)

                mplb.write_input(slab, cwd+new_folder)

                # Writes new INCAR file based on changes made by custodian on the bulk's INCAR.
                # Only change in parameters between slab and bulk should be MAGMOM and ISIF

                if os.path.exists("%s/INCAR.relax2.gz" %(cwd+folder)):
                    incar = Incar.from_file(cwd+folder +'/INCAR.relax2.gz')
                else:
                    incar = Incar.from_file(cwd+folder +'/INCAR.relax2')
                if os.path.exists("%s/OUTCAR.relax2.gz" %(cwd+folder)):
                    out = Outcar(cwd+folder+'/OUTCAR.relax2.gz')
                else:
                    out = Outcar(cwd+folder+'/OUTCAR.relax2')
                if not out:
                    warnings.warn("Magnetization not found in OUTCAR.relax2,gz, "
                                  "may be incomplete, will obtain magmom from "
                                  "OUTCAR.relax1.gz")
                    out = Outcar(cwd+folder+'/OUTCAR.relax1.gz')

                out_mag = out.magnetization
                tot_mag = [mag['tot'] for mag in out_mag]
                magmom = np.mean(tot_mag)
                mag= [magmom]*len(slab)
                incar.__setitem__('MAGMOM', mag)
                incar.__setitem__('ISIF', 2)
                incar.__setitem__('AMIN', 0.01)
                incar.__setitem__('AMIX', 0.2)
                incar.__setitem__('BMIX', 0.001)
                incar.__setitem__('ISTART', 0)
                incar.__setitem__('NELMIN', 8)
                incar.write_file(cwd+new_folder+'/INCAR')

                fw = Firework([RunCustodianTask(dir=new_folder, cwd=cwd,
                                                **custodian_params),
                               VaspSlabDBInsertTask(struct_type="slab_cell",
                                                    loc=new_folder, cwd=cwd, shift=slab.shift,
                                                    surface_area=slab.surface_area,
                                                    vsize=slabs.min_vac_size,
                                                    ssize=slabs.min_slab_size,
                                                    miller_index=miller_index,
                                                    mpid=mpid, conventional_spacegroup=spacegroup,
                                                    **vaspdbinsert_parameters)],
                              name=new_folder)
                FWs.append(fw)

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

        c = Custodian(handlers=handlers, jobs=jobs, max_errors=max_errors, gzipped_output=True, **cust_params)
        output = c.run()

        return FWAction(stored_data=output)

@explicit_serialize
class MoveDirectoryTask(FireTaskBase):
    """
    Basic task to create new directories to move
    and organize completed directories. This will
    prevent the home directory from being filled up.
    """

    required_params = ["cwd", "formula", "miller_index", "mpid", "final_directory"]

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