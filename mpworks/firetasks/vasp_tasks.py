#!/usr/bin/env python

"""
VASP tasks for Materials Project, e.g. VASPWriter and VASP mover
"""

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Potcar, Kpoints
import distutils.core

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class VASPWriterTask(FireTaskBase, FWSerializable):
    
    _fw_name = "VASP Writer Task"
    
    def run_task(self, fw_spec):
        Incar.from_dict(fw_spec['vasp_pmg']['incar']).write_file('INCAR')
        Poscar.from_dict(fw_spec['vasp_pmg']['poscar']).write_file('POSCAR')
        Potcar.from_dict(fw_spec['vasp_pmg']['potcar']).write_file('POTCAR')
        Kpoints.from_dict(fw_spec['vasp_pmg']['kpoints']).write_file('KPOINTS')


class VASPCopyTask(FireTaskBase, FWSerializable):

    _fw_name = "VASP Copy Task"

    def run_task(self, fw_spec):
        copied_files = distutils.dir_util.copy_tree(fw_spec['prev_vasp_dir'], '.')  # distutils was the best code I could find for this task

        return FWAction('CONTINUE', {'copied_files': copied_files})
