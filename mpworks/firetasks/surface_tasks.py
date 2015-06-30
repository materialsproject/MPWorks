## for Surface Energy Calculation
from __future__ import division, unicode_literals

__author__ = "Zihan XU"
__version__ = "0.1"
__email__ = "vivid0036@gmail.com"
__date__ = "6/2/15"

import itertools
import json
import os
import os.path as pth

from pymongo import MongoClient
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities import fw_utilities
from fireworks import explicit_serialize
from pymatgen.core.structure import Structure
from pymatgen.core.metal_slab import get_input_mp, MPSlabVaspInputSet
from pymatgen.io.vaspio.vasp_output import Vasprun, Poscar
from pymatgen.io.vaspio_set import MPVaspInputSet, DictVaspInputSet
from custodian.custodian import Custodian
from custodian.vasp.handlers import VaspErrorHandler, \
    MeshSymmetryErrorHandler,UnconvergedErrorHandler, \
    NonConvergingErrorHandler, PotimErrorHandler
from custodian.vasp.jobs import VaspJob
from matgendb.creator import VaspToDbTaskDrone

from pymatgen.core.surface import Slab, SlabGenerator
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.matproj.rest import MPRester
from monty.json import MontyDecoder
from pymatgen.core.metal_slab import MPSlabVaspInputSet
from pymatgen.io.vaspio.vasp_output import Poscar
import socket



"""
Firework tasks
"""


@explicit_serialize
class VaspDBInsertTask(FireTaskBase):

    required_params = ["host", "port", "user", "password", "database",
                       "collection", "struct_type", "miller_index", "loc"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        miller_index = dec.process_decoded(self.get("miller_index"))
        struct_type = dec.process_decoded(self.get("struct_type"))
        loc = dec.process_decoded(self.get("loc"))


        if not self["host"]:
            self["host"] = "127.0.0.1"

        if not self["port"]:
            self["port"] = 27017

        if not self["database"]:
            self["database"] = "vasp"

        if not self["collection"]:
            self["collection"] = "tasks"


            drone = VaspToDbTaskDrone(host=self["host"], port=self["port"],
                                      user=self["user"], password=self["password"],
                                      database=self["database"], collection=self["collection"],
                                      additional_fields={"author": os.environ.get("USER"),
                                                         "type": struct_type,
                                                         "miller index": miller_index},
                                      use_full_uri=False)
            drone.assimilate(loc)



# debug
# debug
# debug
# debug
@explicit_serialize
class WriteSurfVaspInput(FireTaskBase):
    """writes VASP inputs given elements, hkl,  """

    required_params = ["element", "miller_index", "api_key"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "symprec", "angle_tolerance", "user_incar_settings",
                       "k_product","potcar_functional"]

    def run_task(self, fw_spec):
        dec = MontyDecoder()
        element = dec.process_decoded(self.get("element"))
        miller_index = dec.process_decoded(self.get("miller_index"))
        api_key = dec.process_decoded(self.get("api_key"))
        min_slab_size= dec.process_decoded(self.get("min_slab_size", 10))
        min_vacuum_size = dec.process_decoded(self.get("min_vacuum_size", 10))
        symprec = dec.process_decoded(self.get("symprec", 0.001))
        angle_tolerance = dec.process_decoded(self.get("angle_tolerance", 5))
        user_incar_settings = dec.process_decoded(self.get("user_incar_settings",
                                                           {'ISIF': 2, 'EDIFFG':  -0.05,'EDIFF': 0.0001,
                                                            'ISMEAR': 1,'AMIX': 0.1,'BMIX': 0.0001,
                                                            'AMIX_MAG': 0.4, 'BMIX_MAG': 0.0001,
                                                            'NPAR':4, 'SIGMA': 0.05}))
        k_product = dec.process_decoded(self.get("k_product", 50))
        potcar_functional = dec.process_decoded(self.get("potcar_fuctional", 'PBE'))

        input_structures = get_input_mp(element, miller_index, api_key, min_slab_size,
                                        min_vacuum_size,symprec, angle_tolerance)


        orient_u_cell = input_structures[0]
        slab_cell = input_structures[1]
        mplb_u = MPSlabVaspInputSet(potcar_functional=potcar_functional, bulk = True)
        print "\n>>>> Now creating vasp inputs for a unit cell\n"
        mplb_u.write_input(orient_u_cell, '%s_ucell_k%s_%s%s%s' %(element, k_product,
                                                                  str(miller_index[0]),
                                                                  str(miller_index[1]),
                                                                  str(miller_index[2])))
        print "\n>>>> Now creating vasp inputs for a slab\n"
        mplb_s = MPSlabVaspInputSet(user_incar_settings=user_incar_settings, k_product=k_product,
                                    potcar_functional=potcar_functional, bulk = False)
        mplb_s.write_input(slab_cell, '%s_scell_k%s_%s%s%s' %(element, k_product,
                                                              str(miller_index[0]),
                                                              str(miller_index[1]),
                                                              str(miller_index[2])))


@explicit_serialize
class WriteVaspInputs(FireTaskBase):
    """writes VASP inputs given elements, hkl,  """

    required_params = ["slab", "folder"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "symprec", "angle_tolerance", "user_incar_settings",
                       "k_product","potcar_functional", "bulk"]

    def run_task(self, fw_spec):
        dec = MontyDecoder()
        slab = dec.process_decoded(self.get("slab"))
        folder = dec.process_decoded(self.get("folder"))
        symprec = dec.process_decoded(self.get("symprec", 0.001))
        angle_tolerance = dec.process_decoded(self.get("angle_tolerance", 5))
        user_incar_settings = dec.process_decoded(self.get("user_incar_settings",
                                                           {'ISIF': 2, 'EDIFFG':  -0.05,'EDIFF': 0.0001,
                                                            'ISMEAR': 1,'AMIX': 0.1,'BMIX': 0.0001,
                                                            'AMIX_MAG': 0.4, 'BMIX_MAG': 0.0001,
                                                            'NPAR':4, 'SIGMA': 0.05}))
        k_product = dec.process_decoded(self.get("k_product", 50))
        potcar_functional = dec.process_decoded(self.get("potcar_fuctional", 'PBE'))
        bulk = dec.process_decoded(self.get("bulk", True))

        if bulk:
            mplb_u = MPSlabVaspInputSet(user_incar_settings=user_incar_settings, k_product=k_product,
                                        potcar_functional=potcar_functional, bulk=bulk)
            mplb_u.write_input(slab.oriented_unit_cell, folder)
        else:
            mplb_u = MPSlabVaspInputSet(user_incar_settings=user_incar_settings, k_product=k_product,
                                        potcar_functional=potcar_functional, bulk=bulk)
            contcar = Poscar.from_file("%s/CONTCAR" %(folder.replace('slab', 'bulk')))
            relax_orient_uc = contcar.structure
            slab = SlabGenerator(relax_orient_uc, (0,0,1), min_slab_size=min_slab_size,
                                 min_vacuum_size=min_vacuum_size)
            slab = slab.get_slab()
            mplb_s.write_input(slab, folder)


@explicit_serialize
class WriteSlabVaspInputs(FireTaskBase):
    """writes VASP inputs given elements, hkl,  """

    required_params = ["dir"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "symprec", "angle_tolerance", "user_incar_settings",
                       "k_product","potcar_functional"]

    def run_task(self, fw_spec):
        dec = MontyDecoder()
        dir = dec.process_decoded(self.get("dir"))
        min_slab_size= dec.process_decoded(self.get("min_slab_size", 10))
        min_vacuum_size = dec.process_decoded(self.get("min_vacuum_size", 10))
        symprec = dec.process_decoded(self.get("symprec", 0.001))
        angle_tolerance = dec.process_decoded(self.get("angle_tolerance", 5))
        user_incar_settings = dec.process_decoded(self.get("user_incar_settings",
                                                           {'ISIF': 2, 'EDIFFG':  -0.05,'EDIFF': 0.0001,
                                                            'ISMEAR': 1,'AMIX': 0.1,'BMIX': 0.0001,
                                                            'AMIX_MAG': 0.4, 'BMIX_MAG': 0.0001,
                                                            'NPAR':4, 'SIGMA': 0.05}))
        k_product = dec.process_decoded(self.get("k_product", 50))
        potcar_functional = dec.process_decoded(self.get("potcar_fuctional", 'PBE'))

        contcar = Poscar.from_file("./%s/CONTCAR" %(dir))
        relax_orient_uc = contcar.structure
        slab = SlabGenerator(relax_orient_uc, (0,0,1), min_slab_size=min_slab_size,
                             min_vacuum_size=min_vacuum_size, max_normal_search=1)
        slab = slab.get_slab()

        mplb_s = MPSlabVaspInputSet(user_incar_settings=user_incar_settings, k_product=k_product,
                                    potcar_functional=potcar_functional, bulk=False)

        newdir = dir.replace("ucell", "scell")
        mplb_s.write_input(slab, newdir)


@explicit_serialize
class RunCustodianTask(FireTaskBase):
    """Runs Custodian."""

    required_params = ["jobs"]
    optional_params = ["custodian_params"]

    def run_task(self, fw_spec):

        fw_env = fw_spec.get("_fw_env", {})
        cust_params = self.get("custodian_params", {})
        if fw_env.get('scratch_root'):
            cust_params['scratch_dir'] = os.path.expandvars(
                fw_env['scratch_root'])

        dec = MontyDecoder()
        #handlers = dec.process_decoded(self['handlers'])
        jobs = dec.process_decoded(self['jobs'])
        #validators = [VasprunXMLValidator()]
        handlers = [VaspErrorHandler(), MeshSymmetryErrorHandler(),
                    UnconvergedErrorHandler(), NonConvergingErrorHandler(),
                    PotimErrorHandler()]

        c = Custodian(handlers=handlers, jobs=jobs, max_errors=10, **cust_params)
        output = c.run()

        return FWAction(stored_data=output)

# debug
# debug
# debug
# debug
@explicit_serialize
class SimplerCustodianTask(FireTaskBase):
    """Runs Custodian."""

    required_params = ["dir", "jobs"]
    optional_params = ["custodian_params"]


    def run_task(self, fw_spec):

        dir = dec.process_decoded(self['dir'])
        os.chdir(dir)

        """Runs Custodian."""

        fw_env = fw_spec.get("_fw_env", {})
        cust_params = self.get("custodian_params", {})
        if fw_env.get('scratch_root'):
            cust_params['scratch_dir'] = os.path.expandvars(
                fw_env['scratch_root'])

        dec = MontyDecoder()
        #handlers = dec.process_decoded(self['handlers'])
        jobs = dec.process_decoded(self['jobs'])
        #validators = [VasprunXMLValidator()]
        handlers = [VaspErrorHandler(), MeshSymmetryErrorHandler(),
                    UnconvergedErrorHandler(), NonConvergingErrorHandler(),
                    PotimErrorHandler()]

        c = Custodian(handlers=[], jobs=jobs)
        output = c.run()

        return FWAction(stored_data=output)