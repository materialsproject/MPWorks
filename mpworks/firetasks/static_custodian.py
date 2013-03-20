__author__ = 'weichen'

from custodian.vasp.jobs import VaspJob
from pymatgen.io.vaspio import Poscar, Kpoints, Vasprun
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.core.structure import Structure
from json import load
import os.path


class VaspStaticJob(VaspJob):

    def __init__(self, vasp_command, vasp_spec="bs_static.json"):
        VaspJob.__init__(self, vasp_command, output_file="vasp.out.static",
                         suffix=".static")
        self.vasp_spec = vasp_spec

    def setup(self):
        VaspJob.setup(self)

        try:
            self.vasp_run = Vasprun("vasprun.xml", parse_dos=False,
                                    parse_eigen=False).to_dict
        except Exception as e:
            raise RuntimeError("Can't get valid results from relaxed run")

        with open(os.path.join(os.path.dirname(__file__), self.vasp_spec)) as vs:
            vasp_param = load(vs)
        for p,q in vasp_param["INCAR"].items():
            self.vasp_run['input']['incar'].__setitem__(p, q)

        #set POSCAR with the primitive relaxed structure
        relaxed_struct = self.vasp_run['output']['crystal']
        sym_finder = SymmetryFinder(Structure.from_dict(relaxed_struct), symprec=0.01)
        self.refined_relaxed_struct = sym_finder.get_refined_structure()
        self.primitive_relaxed_struct = sym_finder.get_primitive_standard_structure()
        self.vasp_run['poscar'] = Poscar(self.primitive_relaxed_struct)

        #set KPOINTS
        kpoint_density = vasp_param["KPOINTS"]
        num_kpoints = kpoint_density * self.primitive_relaxed_struct.lattice.reciprocal_lattice.volume
        kpoints=Kpoints.automatic_density(self.primitive_relaxed_struct, num_kpoints *
                                          self.primitive_relaxed_struct.num_sites)
        self.vasp_run['kpoints'] = kpoints

