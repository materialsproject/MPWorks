__author__ = 'weichen'

from json import load
import os.path
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase,FWAction,FireWork
from pymatgen.io.vaspio.vasp_output import Vasprun
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Kpoints
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.core.structure import Structure


class SetupStaticRunTask(FireTaskBase, FWSerializable):
    '''
    Set VASP input sets for static runs, assuming vasp Inputs/Outputs from relax runs are already in the directory
    '''

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