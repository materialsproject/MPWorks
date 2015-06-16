__author__ = 'Qimin'

import os
from monty.os.path import zpath
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_output import Vasprun, Outcar
from pymatgen.io.vaspio_set import MPBSHSEVaspInputSet, MPNonSCFVaspInputSet, MPHSEVaspInputSet

class AddHSEBSTask(FireTaskBase, FWSerializable):
    """
    Set up vasp inputs for HSE band structure calculations
    """
    _fw_name = "Setup HSE band structure Task"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)
        self.line = parameters.get('mode', 'line').lower() == 'line'

    def run_task(self, fw_spec):

        try:
            vasp_run = Vasprun(zpath("vasprun.xml"), parse_dos=False,
                               parse_eigen=False)
            outcar = Outcar(os.path.join(os.getcwd(), zpath("OUTCAR")))
        except Exception as e:
            raise RuntimeError("Can't get valid results from relaxed run: " +
                               str(e))

        structure = MPNonSCFVaspInputSet.get_structure(vasp_run, outcar,
                                                       initial_structure=True)
        user_incar_settings = {"NSW": 0}
        mphsebsvis = MPBSHSEVaspInputSet(user_incar_settings)
        mphsevis = MPHSEVaspInputSet(user_incar_settings)

        mphsebsvis.get_kpoints(structure).write_file("KPOINTS")
        #mphsevis.get_potcar(structure).write_file("POTCAR")
        #mphsevis.get_poscar(structure).write_file("POSCAR")

        return FWAction()