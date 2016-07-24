import os
from monty.os.path import zpath
from custodian.vasp.handlers import UnconvergedErrorHandler
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vasp.outputs import Vasprun, Outcar
from pymatgen.io.vasp.inputs import VaspInput, Incar, Poscar, Kpoints, Potcar
from pymatgen.io.vasp.sets import MPRelaxSet, MPStaticSet, MPNonSCFSet
from pymatgen.symmetry.bandstructure import HighSymmKpath
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer

__author__ = 'Wei Chen, Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Wei Chen'
__email__ = 'weichen@lbl.gov'
__date__ = 'Mar 20, 2013'

module_dir = os.path.dirname(__file__)


class SetupStaticRunTask(FireTaskBase, FWSerializable):
    """
    Set VASP input sets for static runs, assuming vasp Outputs (vasprun.xml
    and OUTCAR) from relax runs are already in the directory
    """

    _fw_name = "Setup Static Task"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)
        self.kpoints_density = parameters.get('kpoints_density', 90)
        self.user_incar_settings = parameters.get('user_incar_settings', {})

    def run_task(self, fw_spec):
        self.user_incar_settings.update({"NPAR": 2})
        # Get kpoint density per vol
        vol = Poscar.from_file("POSCAR").structure.volume
        kppra_vol = self.kpoints_density / vol
        new_set = MPStaticSet.from_prev_calc(
            os.getcwd(),
            user_incar_settings=self.user_incar_settings, 
            reciprocal_density=kppra_vol)
        new_set.write_input('.')
        structure = new_set.structure
        sga = SpacegroupAnalyzer(structure, 0.1)
        return FWAction(stored_data={
            'refined_structure': sga.get_refined_structure().as_dict(),
            'conventional_standard_structure': sga.get_conventional_standard_structure().as_dict(),
            'symmetry_dataset': sga.get_symmetry_dataset(),
            'symmetry_operations': [x.as_dict() for x in sga.get_symmetry_operations()]})


class SetupUnconvergedHandlerTask(FireTaskBase, FWSerializable):
    """
    Assumes the current directory contains an unconverged job. Fixes it and
    runs it
    """

    _fw_name = "Unconverged Handler Task"

    def run_task(self, fw_spec):
        ueh = UnconvergedErrorHandler()
        custodian_out = ueh.correct()
        return FWAction(stored_data={'error_list': custodian_out['errors']})


class SetupNonSCFTask(FireTaskBase, FWSerializable):
    """
    Set up vasp inputs for non-SCF calculations (Uniform [DOS] or band
    structure)
    """
    _fw_name = "Setup non-SCF Task"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)
        self.line = parameters.get('mode', 'line').lower() == 'line'
        self.kpoints_density = parameters.get('kpoints_density', 1000)
        self.kpoints_line_density = parameters.get('kpoints_line_density', 20)

    def run_task(self, fw_spec):
        user_incar_settings= {"NPAR": 2}
        vol = Poscar.from_file("POSCAR").structure.volume
        kppra_vol = self.kpoints_density / vol
        if self.line:
            MPNonSCFSet.from_prev_calc(
                os.getcwd(), mode="Line", copy_chgcar=False,
                user_incar_settings=user_incar_settings,
                kpoints_line_density=self.kpoints_line_density).write_input('.')
            kpath = HighSymmKpath(Poscar.from_file("POSCAR").structure)

            return FWAction(stored_data={"kpath": kpath.kpath,
                                         "kpath_name": kpath.name})
        else:
            MPNonSCFSet.from_prev_calc(
                os.getcwd(), mode="Uniform", copy_chgcar=False,
                user_incar_settings=user_incar_settings,
                reciprocal_density=kppra_vol).write_input('.')
            return FWAction()


class SetupGGAUTask(FireTaskBase, FWSerializable):
    """
    Assuming that GGA inputs/outputs already exist in the directory, set up a
    GGA+U run.
    """
    _fw_name = "Setup GGAU Task"

    def run_task(self, fw_spec):
        chgcar_start = False
        # read the VaspInput from the previous run

        poscar = Poscar.from_file(zpath('POSCAR'))
        incar = Incar.from_file(zpath('INCAR'))

        # figure out what GGA+U values to use and override them
        # LDAU values to use
        mpvis = MPRelaxSet(poscar.structure)
        ggau_incar = mpvis.incar.as_dict()
        incar_updates = {k: ggau_incar[k] for k in ggau_incar.keys() if 'LDAU' in k}

        for k in ggau_incar:
            # update any parameters not set explicitly in previous INCAR
            if k not in incar and k in ggau_incar:
                incar_updates[k] = ggau_incar[k]

        incar.update(incar_updates)  # override the +U keys


        # start from the CHGCAR of previous run
        if os.path.exists('CHGCAR'):
            incar['ICHARG'] = 1
            chgcar_start = True

        # write back the new INCAR to the current directory
        incar.write_file('INCAR')
        return FWAction(stored_data={'chgcar_start': chgcar_start})
