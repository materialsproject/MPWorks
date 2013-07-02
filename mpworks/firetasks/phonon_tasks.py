__author__ = 'weichen'


from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar
from genstrain import DeformGeometry


class SetupFConvergenceTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Force Convergence Task"

    def run_task(self, fw_spec):
        incar = fw_spec['vasp']['incar']
        update_set = {"ENCUT": 600, "EDIFF": 0.00005, "EDIFFG": -0.0005}
        incar.update(update_set)
        if fw_spec['double_kmesh']:
            kpoints = fw_spec['vasp']['kpoints']
            k = [2*k for k in kpoints[kpoints]]
            kpoints['kpoints'] = [k]
        return FWAction()

class SetupElastConstTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup non-SCF Task"

    def run_task(self, fw_spec):
        incar = Incar.from_file("INCAR")
        incar.update({"ISIF": 2})
        incar.write_file("INCAR")
        return FWAction()

