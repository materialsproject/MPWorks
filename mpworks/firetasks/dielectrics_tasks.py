from monty.os.path import zpath


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar, Poscar
from pymatgen.io.vaspio_set import MPStaticDielectricDFPTVaspInputSet


class SetupStaticDielectricsConvergenceTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Static Dielectrics Convergence Task"

    def run_task(self, fw_spec):
        incar = fw_spec['vasp']['incar']
        update_set = {"ENCUT": 600, "EDIFF": 0.00005}
        incar.update(update_set)
        #if fw_spec['double_kmesh']:
        kpoints = fw_spec['vasp']['kpoints']
        k = [2*k for k in kpoints['kpoints'][0]]
        kpoints['kpoints'] = [k]
        return FWAction()


class SetupStaticDielectricsTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Static Dielectrics Task"

    def run_task(self, fw_spec):
        poscar = Poscar.from_file(zpath('POSCAR'))
        incar = mpvis.get_incar(structure=poscar.structure)
        incar.write_file("INCAR") # Over-write the INCAR file with the one for Static Dielectrics
        return FWAction()