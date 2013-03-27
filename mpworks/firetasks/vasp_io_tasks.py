#!/usr/bin/env python

"""

"""
import json
import os
import shutil

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from mpworks.drones.matproj_vaspdrone import MatprojVaspDrone
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Potcar, Kpoints

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class VaspWriterTask(FireTaskBase, FWSerializable):
    """
    Write VASP input files based on the fw_spec
    """

    _fw_name = "Vasp Writer Task"

    def run_task(self, fw_spec):
        Incar.from_dict(fw_spec['vasp']['incar']).write_file('INCAR')
        Poscar.from_dict(fw_spec['vasp']['poscar']).write_file('POSCAR')
        Potcar.from_dict(fw_spec['vasp']['potcar']).write_file('POTCAR')
        Kpoints.from_dict(fw_spec['vasp']['kpoints']).write_file('KPOINTS')


class VaspCopyTask(FireTaskBase, FWSerializable):
    """
    Copy the VASP run directory in 'prev_vasp_dir' to the current dir
    """

    _fw_name = "Vasp Copy Task"

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


class VaspToDBTask(FireTaskBase, FWSerializable):
    """
    Enter the VASP run directory in 'prev_vasp_dir' to the database.
    """

    _fw_name = "Vasp to Database Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'parse_dos', 'additional_fields', and 'update_duplicates'
        """
        self.parameters = parameters  # store the parameters explicitly set by the user

        parameters = parameters if parameters else {}
        self.parse_dos = parameters.get('parse_dos', False)
        self.additional_fields = parameters.get('additional_fields', None)
        self.update_duplicates = parameters.get('update_duplicates', False)

    def run_task(self, fw_spec):
        prev_dir = fw_spec['prev_vasp_dir']

        # TODO: should the PATH point to the file not the dir? probably...

        # get the directory containing the db file
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'db.json')
        with open(db_path) as f:
            db_creds = json.load(f)
            drone = MatprojVaspDrone(host=db_creds['host'], port=db_creds['port'], database=db_creds['database'],
                                     user=db_creds['admin_user'], password=db_creds['admin_password'],
                                     collection=db_creds['collection'], parse_dos=self.parse_dos,
                                     additional_fields=self.additional_fields, update_duplicates=self.update_duplicates)
            drone.assimilate(prev_dir)

        stored_data = {}  # TODO: decide what data to store (if any)
        return FWAction('MODIFY', stored_data, {'dict_update': {'prev_vasp_dir': prev_dir}})




