from json import load
import os.path
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase,FWAction
from pymatgen.io.vaspio.vasp_output import Vasprun
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Kpoints, VaspInput
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.core.structure import Structure


__author__ = 'Wei Chen, Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Wei Chen'
__email__ = 'weichen@lbl.gov'
__date__ = 'Mar 20, 2013'


class SetupStaticRunTask(FireTaskBase, FWSerializable):
    """
    Set VASP input sets for static runs, assuming vasp Inputs/Outputs from relax runs are already in the directory
    """

    _fw_name = "Setup Static Run Task"

    def run_task(self, fw_spec):

        try:
            vasp_run = Vasprun("vasprun.xml", parse_dos=False,
                               parse_eigen=False).to_dict
        except Exception as e:
            raise RuntimeError("Can't get valid results from relaxed run")

        with open(os.path.join(os.path.dirname(__file__), "bs_static.json")) as vs:
            vasp_param = load(vs)
        for p,q in vasp_param["INCAR"].items():
            vasp_run['input']['incar'].__setitem__(p, q)

        #set POSCAR with the primitive relaxed structure
        relaxed_struct = vasp_run['output']['crystal']
        sym_finder = SymmetryFinder(Structure.from_dict(relaxed_struct), symprec=0.01)
        refined_relaxed_struct = sym_finder.get_refined_structure()
        primitive_relaxed_struct = sym_finder.get_primitive_standard_structure()
        Poscar(primitive_relaxed_struct).write_file("POSCAR")

        #set KPOINTS
        kpoint_density = vasp_param["KPOINTS"]
        num_kpoints = kpoint_density * primitive_relaxed_struct.lattice.reciprocal_lattice.volume
        Kpoints.automatic_density(primitive_relaxed_struct, num_kpoints *
                                                            primitive_relaxed_struct.num_sites).write_file("KPOINTS")

        #set INCAR with static run config
        with open(os.path.join(os.path.dirname(__file__), "bs_static.json")) as vs:
            vasp_param = load(vs)
        for p,q in vasp_param["INCAR"].items():
            vasp_run['input']['incar'].__setitem__(p, q)
        Incar.from_dict(vasp_run['input']['incar']).write_file("INCAR")

        return FWAction('CONTINUE',{'refined_struct':refined_relaxed_struct})


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