__author__ = 'weichen'
from json import load

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase,FWAction,FireWork
from pymatgen.io.vaspio import Poscar,Kpoints,Vasprun
from pymatgen.symmetry.finder import SymmetryFinder
from mpworks.firetasks.vaspwriter_task import VASPWriterTask


class VASPStaticRunTask(FireTaskBase, FWSerializable):
    '''
    Set VASP input sets for static runs
    '''

    _fw_name = "VASP Static Run Task"

    def run_task(self, fw_spec):
        #set INCAR with static run config
        vasp_param=load(open("bs_static.json"))
        for p,q in vasp_param["INCAR"]:
            fw_spec['vasp']['incar'].__setitem__(p,q)

        try:
            self._vasprun=Vasprun("vasprun_xml")
        except Exception:
            print "Can't get valid results from relaxed run"

        #set POSCAR with the primitive relaxed structure
        relaxed_struct=self._vasprun.final_structure
        sym_finder=SymmetryFinder(relaxed_struct, symprec=0.01)
        self._refined_relaxed_struct=sym_finder.get_refined_structure()
        self._primitive_relaxed_struct=sym_finder.get_primitive_standard_structure()
        fw_spec['vasp']['poscar']=Poscar(self._primitive_relaxed_struct)

        #set KPOINTS
        kpoint_density=vasp_param["KPOINTS"]
        num_kpoints=kpoint_density*self._primitive_relaxed_struct.reciprocal_lattice.volume
        kpoints=Kpoints.automatic_density(
            self._primitive_relaxed_struct,num_kpoints * self._primitive_relaxed_struct.num_sites)
        fw_spec['vasp']['kpoints'] = kpoints

        fw_spec['static'] = True

        static_run_fw = FireWork(VASPWriterTask(),fw_spec)
        return FWAction('CREATE',{'refined_struct':self._refined_relaxed_struct},{'create_fw':static_run_fw})