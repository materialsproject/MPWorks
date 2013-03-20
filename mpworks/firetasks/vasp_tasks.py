#!/usr/bin/env python

"""
VASP tasks for Materials Project, e.g. VASPWriter and VASP mover
"""
import os
import shutil

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Potcar, Kpoints, VaspInput
import distutils.core
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet

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

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'extension', 'use_CONTCAR', and 'files'
        """
        self.parameters = parameters  # store the parameters explicitly set by the user

        default_files = ['INCAR', 'POSCAR', 'KPOINTS', 'POTCAR', 'OUTCAR', 'vasprun.xml', 'CHGCAR', 'OSZICAR']
        parameters = parameters if parameters else {}
        self.files = parameters.get('files', default_files)  # files to move
        self.extension = parameters.get('extension', '')  # e.g., 'relax2' means to move relax2 files
        self.use_contcar = parameters.get('use_CONTCAR', True)  # whether to move CONTCAR to POSCAR

    def run_task(self, fw_spec):
        prev_dir = fw_spec['prev_vasp_dir']

        for file in self.files:
            prev_filename = os.path.join(prev_dir, file + self.extension)
            if file == 'POTCAR':
                prev_filename = os.path.join(prev_dir, file)  # no extension gets added to POTCAR files
            dest_file = 'POSCAR' if file == 'CONTCAR' and self.use_contcar else file
            print 'COPYING', prev_filename, dest_file
            shutil.copy2(prev_filename, dest_file)

        return FWAction('CONTINUE', {'copied_files': self.files})


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
