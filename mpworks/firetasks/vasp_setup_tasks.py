from json import load
import os.path
import traceback
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase,FWAction
from pymatgen.io.vaspio.vasp_output import Vasprun, Outcar
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Kpoints, VaspInput
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.core.structure import Structure
from pymatgen.symmetry.bandstructure import HighSymmKpath
import numpy as np


__author__ = 'Wei Chen, Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Wei Chen'
__email__ = 'weichen@lbl.gov'
__date__ = 'Mar 20, 2013'

module_dir = os.path.dirname(__file__)


class SetupStaticRunTask(FireTaskBase, FWSerializable):
    """
    Set VASP input sets for static runs, assuming vasp Inputs/Outputs from
    relax runs are already in the directory
    """

    _fw_name = "Setup Static Run Task"

    def run_task(self, fw_spec):

        try:
            vasp_run = Vasprun("vasprun.xml", parse_uniform=False,
                               parse_eigen=False).to_dict
        except:
            traceback.format_exc()
            raise RuntimeError("Can't get valid results from relaxed run")

        with open(os.path.join(module_dir, "bs_static.json")) as vs:
            vasp_param = load(vs)
        vasp_run['input']['incar'].update(vasp_param["INCAR"])

        #set POSCAR with the primitive relaxed structure
        relaxed_struct = vasp_run['output']['crystal']
        sym_finder = SymmetryFinder(Structure.from_dict(relaxed_struct),
                                    symprec=0.01)
        refined_relaxed_struct = sym_finder.get_refined_structure()
        primitive_relaxed_struct = sym_finder.get_primitive_standard_structure()
        Poscar(primitive_relaxed_struct).write_file("POSCAR")

        #set KPOINTS
        kpoint_density = vasp_param["KPOINTS"]
        num_kpoints = kpoint_density * \
            primitive_relaxed_struct.lattice.reciprocal_lattice.volume
        kpoints = Kpoints.automatic_density(
            primitive_relaxed_struct,
            num_kpoints * primitive_relaxed_struct.num_sites)
        kpoints.write_file("KPOINTS")

        #set INCAR with static run config
        with open(os.path.join(module_dir, "bs_static.json")) as vs:
            vasp_param = load(vs)
        vasp_run['input']['incar'].update(vasp_param["INCAR"])
        Incar(vasp_run['input']['incar']).write_file("INCAR")

        # redo POTCAR - this is necessary whenever you change a Structure
        # because element order might change!! (learned the hard way...)

        # TODO: FIXME: if order of POSCAR atoms changes, the MAGMOMs in INCAR might be incorrect
        # TODO: FIXME: if order of POSCAR atoms changes, the LDAU in INCAR might be incorrect
        potcar = MaterialsProjectVaspInputSet().get_potcar(
            primitive_relaxed_struct)
        potcar.write_file("POTCAR")

        return FWAction('CONTINUE',
                        {'refined_struct': refined_relaxed_struct.to_dict})


class SetupUniformRunTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Uniform Run Task"

    def run_task(self, fw_spec):
        with open(os.path.join(os.path.dirname(__file__), "uniform.json")) as vs:
            vasp_param = load(vs)

        try:
            incar = Incar.from_file("INCAR")
            poscar = Poscar.from_file("POSCAR")
        except Exception as e:
            raise RuntimeError(e)

        incar.update(vasp_param["INCAR"])
        incar.write_file("INCAR")

        kpoint_density = vasp_param["KPOINTS"]
        struct = poscar.structure
        num_kpoints = kpoint_density * struct.lattice.reciprocal_lattice.volume
        kpoints = Kpoints.automatic_density(struct,
                                            num_kpoints*struct.num_sites)
        kpoints.write_file("KPOINTS")

        return FWAction('CONTINUE')


class SetupBSTask(FireTaskBase, FWSerializable):
    """
    Set up vasp inputs for bandstructure calculations
    Specify KPOINTS scheme with fw_spec['BS']
    fw_spec['BS'] = 'line': KPOINTS along symmetry lines
    fw_spec['BS'] = 'uniform': KPOINTS with uniform grid
    """
    _fw_name = "Setup Bandstructure Run"

    def run_task(self, fw_spec):

        with open(os.path.join(module_dir, "bandstructure.json")) as vs:
            vasp_param = load(vs)

        try:
            incar = Incar.from_file("INCAR")
        except Exception as e:
            raise RuntimeError(e)

        try:
            vasp_run = Vasprun("vasprun.xml", parse_uniform=False,
                               parse_eigen=False).to_dict
            outcar = Outcar(os.path.join(os.getcwd(),"OUTCAR")).to_dict
        except Exception as e:
            raise RuntimeError("Can't get valid results from relaxed run: " + str(e))

        #Set up INCAR (including set ISPIN and NBANDS)
        incar.update(vasp_param["INCAR"].items())
        site_magmon = np.array([i['tot'] for i in outcar['magnetization']])
        ispin = 2 if np.any(site_magmon[np.abs(site_magmon) > 0.02]) else 1
        incar["ISPIN"] = ispin
        nbands = int(np.ceil(vasp_run["input"]["parameters"]["NBANDS"] * 1.2))
        incar["NBANDS"] = nbands
        incar.write_file("INCAR")

        #Set up KPOINTS (make sure cart/reciprocal is correct!)
        struct = Structure.from_dict(vasp_run['output']['crystal'])
        kpath = HighSymmKpath(struct)
        if fw_spec['BS'] == "line":
            cart_k_points = kpath.get_kpoints()
            kpoints = Kpoints(comment="Bandstructure along symmetry lines",
                              style="Cartesian",
                              num_kpts=len(cart_k_points), kpts=cart_k_points,
                              kpts_weights=[1]*len(cart_k_points))
        elif fw_spec['BS'] == "uniform":
            kpoint_density = vasp_param["KPOINTS"]
            num_kpoints = kpoint_density * struct.lattice.reciprocal_lattice.volume
            kpoints = Kpoints.automatic_density(struct, num_kpoints*struct.num_sites)
            '''
            ir_kpts = SymmetryFinder(struct, symprec=0.01).get_ir_kpoints(kpts)
            kpoints = Kpoints (comment="Bandstructure on uniform grid",
                               style="Reciprocal",
                               num_kpts=len(ir_kpts), kpts=ir_kpts,
                               kpt_weights=[1]*len(ir_kpts))
            '''
        kpoints.write_file("KPOINTS")

        return FWAction("CONTINUE", {"kpath": kpath.kpath, "kpath_name":kpath.name})


class SetupGGAUTask(FireTaskBase, FWSerializable):
    """
    Assuming that GGA inputs/outputs already exist in the directory, set up a GGA+U run.
    """
    _fw_name = "Setup GGAU Task"

    def run_task(self, fw_spec):

        vi = VaspInput.from_directory(".")  # read the VaspInput from the previous run

        # figure out what GGA+U values to use and override them
        mpvis = MaterialsProjectVaspInputSet()
        incar = mpvis.get_incar(vi['POSCAR'].structure).to_dict
        incar_updates = {k: incar[k] for k in incar.keys() if 'LDAU' in k}  # LDAU values to use
        vi['INCAR'].update(incar_updates)  # override the +U keys

        # start from the CHGCAR of previous run
        if os.path.exists('CHGCAR'):
            vi['INCAR']['ICHARG'] = 1

        vi["INCAR"].write_file("INCAR")  # write back the new INCAR to the current directory

        return FWAction('CONTINUE')