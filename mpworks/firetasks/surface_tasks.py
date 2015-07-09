## for Surface Energy Calculation
from __future__ import division, unicode_literals

__author__ = "Zihan XU"
__version__ = "0.1"
__email__ = "vivid0036@gmail.com"
__date__ = "6/2/15"

import os

from fireworks.core.firework import FireTaskBase, FWAction, Firework
from fireworks import explicit_serialize
from pymatgen.io.vaspio.vasp_output import Vasprun, Poscar
from custodian.custodian import Custodian
from custodian.vasp.jobs import VaspJob
from matgendb.creator import VaspToDbTaskDrone

from pymatgen.core.surface import SlabGenerator
from monty.json import MontyDecoder
from pymatgen.core.metal_slab import MPSlabVaspInputSet

"""
Firework tasks
"""


@explicit_serialize
class VaspDBInsertTask(FireTaskBase):

    required_params = ["host", "port", "user", "password",
                       "database", "collection", "struct_type",
                       "miller_index", "loc"]
    optional_params = ["surface_area", "shift", "vsize", "ssize"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        miller_index = str(dec.process_decoded(self.get("miller_index")))
        struct_type = dec.process_decoded(self.get("struct_type"))
        loc = dec.process_decoded(self.get("loc"))
        surface_area = dec.process_decoded(self.get("surface_area", None))
        shift = dec.process_decoded(self.get("shift", None))
        vsize = dec.process_decoded(self.get("vsize", None))
        ssize = dec.process_decoded(self.get("ssize", None))


        if not self["host"]:
            self["host"] = "127.0.0.1"

        if not self["port"]:
            self["port"] = 27017

        if not self["database"]:
            self["database"] = "vasp"

        if not self["collection"]:
            self["collection"] = "tasks"

        additional_fields={"author": os.environ.get("USER"),
                           "structure_type": struct_type,
                           "miller_index": miller_index,
                           "surface_area": surface_area, "shift": shift,
                           "vsize": vsize, "ssize": ssize}

        drone = VaspToDbTaskDrone(host=self["host"], port=self["port"],
                                  user=self["user"], password=self["password"],
                                  database=self["database"], use_full_uri=False,
                                  additional_fields=additional_fields,
                                  collection=self["collection"])
        drone.assimilate(loc)


@explicit_serialize
class WriteUCVaspInputs(FireTaskBase):
    """writes VASP inputs given elements, hkl,  """

    required_params = ["oriented_ucell", "folder"]
    optional_params = ["angle_tolerance", "user_incar_settings",
                       "k_product","potcar_functional", "symprec"]

    def run_task(self, fw_spec):
        dec = MontyDecoder()
        oriented_ucell = dec.process_decoded(self.get("oriented_ucell"))
        folder = dec.process_decoded(self.get("folder"))
        symprec = dec.process_decoded(self.get("symprec", 0.001))
        angle_tolerance = dec.process_decoded(self.get("angle_tolerance", 5))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings",
                                         MPSlabVaspInputSet().incar_settings))
        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_fuctional", 'PBE'))

        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  k_product=k_product, bulk=True,
                                  potcar_functional=potcar_functional)
        mplb.write_input(oriented_ucell, folder)


@explicit_serialize
class WriteSlabVaspInputs(FireTaskBase):
    """writes VASP inputs given elements, hkl,  """

    required_params = ["folder", "custodian_params", "vaspdbinsert_parameters"]
    optional_params = ["min_slab_size", "min_vacuum_size",
                       "angle_tolerance", "user_incar_settings",
                       "k_product","potcar_functional", "symprec"
                       "terminations"]

    def run_task(self, fw_spec):
        dec = MontyDecoder()
        folder = dec.process_decoded(self.get("folder"))
        symprec = dec.process_decoded(self.get("symprec", 0.001))
        angle_tolerance = dec.process_decoded(self.get("angle_tolerance", 5))
        terminations = dec.process_decoded(self.get("terminations", False))
        custodian_params = dec.process_decoded(self.get("custodian_params"))
        vaspdbinsert_parameters = dec.process_decoded(self.get("vaspdbinsert_parameters"))

        user_incar_settings = \
            dec.process_decoded(self.get("user_incar_settings",
                                         MPSlabVaspInputSet().incar_settings))
        k_product = \
            dec.process_decoded(self.get("k_product", 50))
        potcar_functional = \
            dec.process_decoded(self.get("potcar_fuctional", 'PBE'))
        min_slab_size = dec.process_decoded(self.get("min_slab_size", 10))
        min_vacuum_size = dec.process_decoded(self.get("min_vacuum_size", 10))

        mplb = MPSlabVaspInputSet(user_incar_settings=user_incar_settings,
                                  k_product=k_product,
                                    potcar_functional=potcar_functional)

        contcar = Poscar.from_file("%s/CONTCAR" %(folder))
        relax_orient_uc = contcar.structure
        slabs = SlabGenerator(relax_orient_uc, (0,0,1),
                             min_slab_size=min_slab_size,
                             min_vacuum_size=min_vacuum_size)
        slab_list = slabs.get_slabs() if terminations else [slabs.get_slab()]

        FWs = []
        for slab in slab_list:
            new_folder = folder.replace('bulk', 'slab')+'_shift%s' %(slab.shift)
            mplb.write_input(slab, new_folder)
            fw = Firework([RunCustodianTask(dir=new_folder, **custodian_params),
                           VaspDBInsertTask(struct_type="slab_cell",
                           loc=new_folder, surface_area=slab.surface_area,
                           shift=slab.shift, vsize=slabs.min_vac_size,
                           ssize=slabs.min_slab_size, **vaspdbinsert_parameters)])
            FWs.append(fw)

        return FWAction(additions=FWs)


@explicit_serialize
class RunCustodianTask(FireTaskBase):
    """Runs Custodian."""

    required_params = ["dir", "jobs"]
    optional_params = ["custodian_params", "handlers"]

    def run_task(self, fw_spec):

        dec = MontyDecoder()
        dir = dec.process_decoded(self['dir'])
        os.chdir(dir)
        handlers = dec.process_decoded(self.get('handlers', []))
        jobs = dec.process_decoded(self['jobs'])

        fw_env = fw_spec.get("_fw_env", {})
        cust_params = self.get("custodian_params", {})
        if fw_env.get('scratch_root'):
            cust_params['scratch_dir'] = os.path.expandvars(
                fw_env['scratch_root'])

        c = Custodian(handlers=handlers, jobs=[jobs], **cust_params)
        output = c.run()

        return FWAction(stored_data=output)