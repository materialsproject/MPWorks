import os
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_output import Vasprun, Outcar
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Kpoints, VaspInput
from pymatgen.io.vaspio_set import MPVaspInputSet, MPStaticVaspInputSet, MPNonSCFVaspInputSet
from pymatgen.symmetry.bandstructure import HighSymmKpath

__author__ = 'Wei Chen, Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Wei Chen'
__email__ = 'weichen@lbl.gov'
__date__ = 'Mar 20, 2013'

module_dir = os.path.dirname(__file__)


class SetupStaticRunTask(FireTaskBase, FWSerializable):
    """
    Set VASP input sets for static runs, assuming vasp Outputs (vasprun.xml and OUTCAR) from
    relax runs are already in the directory
    """

    _fw_name = "Setup Static Task"

    def run_task(self, fw_spec):
        user_incar_settings = {"NPAR": 2}

        MPStaticVaspInputSet.from_previous_vasp_run(os.getcwd(),
                                                    user_incar_settings=user_incar_settings)
        structure = MPStaticVaspInputSet.get_structure(Vasprun("vasprun.xml"), Outcar("OUTCAR"),
                                                       initial_structure=False,
                                                       refined_structure=True)
        # redo POTCAR - this is necessary whenever you change a Structure
        # because element order might change!! (learned the hard way...) -AJ

        return FWAction(stored_data={'refined_struct': structure[1].to_dict})


class SetupNonSCFTask(FireTaskBase, FWSerializable):
    """
    Set up vasp inputs for non-SCF calculations (Uniform [DOS] or band structure)
    """
    _fw_name = "Setup non-SCF Task"

    def __init__(self, parameters=None):
        """

        :param parameters:
        """
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user
        self.line = parameters.get('mode', 'line').lower() == 'line'

    def run_task(self, fw_spec):

        try:
            vasp_run = Vasprun("vasprun.xml", parse_dos=False,
                               parse_eigen=False)
            outcar = Outcar(os.path.join(os.getcwd(), "OUTCAR"))
        except Exception as e:
            raise RuntimeError("Can't get valid results from relaxed run: " + str(e))

<<<<<<< HEAD
        user_incar_settings = MPNonSCFVaspInputSet.get_incar_settings(vasp_run, outcar)
        user_incar_settings.update({"NPAR": 2})
        structure = MPNonSCFVaspInputSet.get_structure(vasp_run, outcar, initial_structure=True)

        if self.line:
            mpnscfvip = MPNonSCFVaspInputSet(user_incar_settings, mode="Line")
            for k, v in mpnscfvip.get_all_vasp_input(structure, generate_potcar=True).items():
                v.write_file(os.path.join(os.getcwd(), k))
            kpath = HighSymmKpath(structure)
        else:
            mpnscfvip = MPNonSCFVaspInputSet(user_incar_settings, mode="Uniform")
            for k, v in mpnscfvip.get_all_vasp_input(structure, generate_potcar=True).items():
=======
        user_incar_settings= MaterialsProjectNonSCFVaspInputSet.get_incar_settings(vasp_run, outcar)
        user_incar_settings.update({"NPAR":2})
        structure = MaterialsProjectNonSCFVaspInputSet.get_structure(vasp_run, outcar, initial_structure=True)

        if self.line:
            mpnscfvip = MaterialsProjectNonSCFVaspInputSet(user_incar_settings, mode="Line")
            for k,v in mpnscfvip.get_all_vasp_input(structure, generate_potcar=True).items():
                v.write_file(os.path.join(os.getcwd(), k))
            kpath = HighSymmKpath(structure)
        else:
            mpnscfvip = MaterialsProjectNonSCFVaspInputSet(user_incar_settings, mode="Uniform")
            for k,v in mpnscfvip.get_all_vasp_input(structure, generate_potcar=True).items():
>>>>>>> 45582525ed6da99d77ea8432d396f09bbd45045a
                v.write_file(os.path.join(os.getcwd(), k))

        if self.line:
            return FWAction(stored_data={"kpath": kpath.kpath, "kpath_name": kpath.name})
        else:
            return FWAction()


class SetupGGAUTask(FireTaskBase, FWSerializable):
    """
    Assuming that GGA inputs/outputs already exist in the directory, set up a GGA+U run.
    """
    _fw_name = "Setup GGAU Task"

    def run_task(self, fw_spec):
        chgcar_start = False

        vi = VaspInput.from_directory(".")  # read the VaspInput from the previous run

        # figure out what GGA+U values to use and override them
        mpvis = MPVaspInputSet()
        incar = mpvis.get_incar(vi['POSCAR'].structure).to_dict
        incar_updates = {k: incar[k] for k in incar.keys() if 'LDAU' in k}  # LDAU values to use
        vi['INCAR'].update(incar_updates)  # override the +U keys

        # start from the CHGCAR of previous run
        if os.path.exists('CHGCAR'):
            vi['INCAR']['ICHARG'] = 1
            chgcar_start = True

        vi['INCAR'].write_file('INCAR')  # write back the new INCAR to the current directory

        return FWAction(stored_data={'chgcar_start': chgcar_start})