#!/usr/bin/env python

"""
VASP tasks for Materials Project, e.g. VASPWriter and VASP mover
"""
import os

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Potcar, Kpoints, VaspInput
import distutils.core

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class VASPWriterTask(FireTaskBase, FWSerializable):
    """
    Write VASP input files based on the fw_spec
    """

    _fw_name = "VASP Writer Task"

    def run_task(self, fw_spec):
        Incar.from_dict(fw_spec['vasp']['incar']).write_file('INCAR')
        Poscar.from_dict(fw_spec['vasp']['poscar']).write_file('POSCAR')
        Potcar.from_dict(fw_spec['vasp']['potcar']).write_file('POTCAR')
        Kpoints.from_dict(fw_spec['vasp']['kpoints']).write_file('KPOINTS')


class VASPCopyTask(FireTaskBase, FWSerializable):
    """
    Copy the VASP run directory in 'prev_vasp_dir' to the current dir
    """

    _fw_name = "VASP Copy Task"

    def run_task(self, fw_spec):
        # TODO: allow VASPCopyTask to have internal parameters that specify which files to copy
        # TODO: relax2 option
        # TODO: CONTCAR -> POSCAR option

        copied_files = distutils.dir_util.copy_tree(fw_spec['prev_vasp_dir'], '.')  # distutils > shutil for this
        return FWAction('CONTINUE', {'copied_files': copied_files})


class SetupGGAUTask(FireTaskBase, FWSerializable):
    """
    Assuming that GGA inputs/outputs already exist in the directory, set up a GGA+U run.
    """
    _fw_name = "Setup GGAU Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) the INCAR params to override. LDAU values must be listed here!
        """
        if not parameters or 'LDAUU' not in parameters:
            raise ValueError('must specify +U values!')

        self.parameters = parameters

    def run_task(self, fw_spec):

        vi = VaspInput.from_directory(".")  # read the VaspInput from the previous run

        vi['INCAR'].update(self.parameters)  # override the +U keys
        if os.path.exists('CHGCAR'):
            vi['INCAR']['ICHARG'] = 1  # start from the CHGCAR of previous run

        vi["INCAR"].write_file("INCAR")  # write back the new INCAR to the current directory

        return FWAction('CONTINUE', {'incar_overrides': self.parameters})
