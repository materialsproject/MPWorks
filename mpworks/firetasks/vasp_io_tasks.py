#!/usr/bin/env python

"""

"""
import json
import os
import shutil

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction
from mpworks.drones.mp_vaspdrone import MPVaspDrone
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
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user

        default_files = ['INCAR', 'POSCAR', 'KPOINTS', 'POTCAR', 'OUTCAR',
                         'vasprun.xml', 'CHGCAR', 'OSZICAR']
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

        return FWAction(stored_data={'copied_files': self.files})


class VaspToDBTask(FireTaskBase, FWSerializable):
    """
    Enter the VASP run directory in 'prev_vasp_dir' to the database.
    """

    _fw_name = "Vasp to Database Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'parse_uniform', 'additional_fields', and 'update_duplicates'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)

        self.parse_uniform = self.get('parse_uniform', False)
        self.additional_fields = self.get('additional_fields', {})
        self.update_duplicates = self.get('update_duplicates', False)

    def run_task(self, fw_spec):
        prev_dir = fw_spec['prev_vasp_dir']
        update_spec={'prev_vasp_dir': prev_dir}
        # get the directory containing the db file
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')

        with open(db_path) as f:
            db_creds = json.load(f)
            drone = MPVaspDrone(
                host=db_creds['host'], port=db_creds['port'],
                database=db_creds['database'], user=db_creds['admin_user'],
                password=db_creds['admin_password'],
                collection=db_creds['collection'], parse_dos=self.parse_uniform,
                additional_fields=self.additional_fields,
                update_duplicates=self.update_duplicates)
            t_id, d = drone.assimilate(prev_dir)

        mpsnl = d['snl_final'] if 'snl_final' in d else d['snl']
        snlgroup_id = d['snlgroup_id_final'] if 'snlgroup_id_final' in d else d['snlgroup_id']
        update_spec.update({'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id})

        print 'ENTERED task id:', t_id
        stored_data = {'task_id': t_id}
        if d['state'] == 'successful':
            return FWAction(stored_data=stored_data, update_spec=update_spec)
        return FWAction(stored_data=stored_data, defuse_children=True)
