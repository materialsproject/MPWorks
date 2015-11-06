#!/usr/bin/env python

"""

"""
import gzip
import json
import logging
import os
import shutil
import sys
from monty.os.path import zpath
from custodian.vasp.handlers import UnconvergedErrorHandler
from fireworks.core.launchpad import LaunchPad

from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.core.firework import FireTaskBase, FWAction, Firework, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.drones.mp_vaspdrone import MPVaspDrone
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.vasp_setup_tasks import SetupUnconvergedHandlerTask
from mpworks.workflows.wf_settings import QA_VASP, QA_DB, MOVE_TO_GARDEN_PROD, MOVE_TO_GARDEN_DEV
from mpworks.workflows.wf_utils import last_relax, get_loc, move_to_garden
from pymatgen import Composition
from pymatgen.io.vasp.inputs import Incar, Poscar, Potcar, Kpoints
from pymatgen.matproj.snl import StructureNL

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
        fw_spec['vasp']['incar'].write_file('INCAR')
        fw_spec['vasp']['poscar'].write_file('POSCAR')
        fw_spec['vasp']['potcar'].write_file('POTCAR')
        fw_spec['vasp']['kpoints'].write_file('KPOINTS')


class VaspCopyTask(FireTaskBase, FWSerializable):
    """
    Copy the VASP run directory in 'prev_vasp_dir' to the current dir
    """

    _fw_name = "Vasp Copy Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'use_CONTCAR', and 'files'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)  # store the parameters explicitly set by the user

        default_files = ['INCAR', 'POSCAR', 'KPOINTS', 'POTCAR', 'OUTCAR',
                         'vasprun.xml', 'OSZICAR']

        if not parameters.get('skip_CHGCAR'):
            default_files.append('CHGCAR')

        self.missing_CHGCAR_OK = parameters.get('missing_CHGCAR_OK', True)

        self.files = parameters.get('files', default_files)  # files to move
        self.use_contcar = parameters.get('use_CONTCAR', True)  # whether to move CONTCAR to POSCAR

        if self.use_contcar:
            self.files.append('CONTCAR')
            self.files = [x for x in self.files if x != 'POSCAR']  # remove POSCAR

    def run_task(self, fw_spec):
        prev_dir = get_loc(fw_spec['prev_vasp_dir'])

        if '$ALL' in self.files:
            self.files = os.listdir(prev_dir)

        for file in self.files:
            prev_filename = last_relax(os.path.join(prev_dir, file))
            dest_file = 'POSCAR' if file == 'CONTCAR' and self.use_contcar else file
            if prev_filename.endswith('.gz'):
                dest_file += '.gz'

            print 'COPYING', prev_filename, dest_file
            if self.missing_CHGCAR_OK and 'CHGCAR' in dest_file and not os.path.exists(zpath(prev_filename)):
                print 'Skipping missing CHGCAR'
            else:
                shutil.copy2(prev_filename, dest_file)
                if '.gz' in dest_file:
                    # unzip dest file
                    f = gzip.open(dest_file, 'rb')
                    file_content = f.read()
                    with open(dest_file[0:-3], 'wb') as f_out:
                        f_out.writelines(file_content)
                    f.close()
                    os.remove(dest_file)



        return FWAction(stored_data={'copied_files': self.files})


class VaspToDBTask(FireTaskBase, FWSerializable):
    """
    Enter the VASP run directory in 'prev_vasp_dir' to the database.
    """

    _fw_name = "Vasp to Database Task"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'additional_fields', and 'update_duplicates'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)

        self.additional_fields = self.get('additional_fields', {})
        self.update_duplicates = self.get('update_duplicates', False)  # off so DOS/BS doesn't get entered twice

    def run_task(self, fw_spec):
        if '_fizzled_parents' in fw_spec and not 'prev_vasp_dir' in fw_spec:
            prev_dir = get_loc(fw_spec['_fizzled_parents'][0]['launches'][0]['launch_dir'])
            update_spec = {}  # add this later when creating new FW
            fizzled_parent = True
            parse_dos = False
        else:
            prev_dir = get_loc(fw_spec['prev_vasp_dir'])
            update_spec = {'prev_vasp_dir': prev_dir,
                           'prev_task_type': fw_spec['prev_task_type'],
                           'run_tags': fw_spec['run_tags'], 'parameters': fw_spec.get('parameters')}
            fizzled_parent = False
            parse_dos = 'Uniform' in fw_spec['prev_task_type']
        if 'run_tags' in fw_spec:
            self.additional_fields['run_tags'] = fw_spec['run_tags']
        else:
            self.additional_fields['run_tags'] = fw_spec['_fizzled_parents'][0]['spec']['run_tags']

        if MOVE_TO_GARDEN_DEV:
            prev_dir = move_to_garden(prev_dir, prod=False)

        elif MOVE_TO_GARDEN_PROD:
            prev_dir = move_to_garden(prev_dir, prod=True)

        # get the directory containing the db file
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('MPVaspDrone')
        logger.setLevel(logging.INFO)
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(getattr(logging, 'INFO'))
        logger.addHandler(sh)

        with open(db_path) as f:
            db_creds = json.load(f)
            drone = MPVaspDrone(
                host=db_creds['host'], port=db_creds['port'],
                database=db_creds['database'], user=db_creds['admin_user'],
                password=db_creds['admin_password'],
                collection=db_creds['collection'], parse_dos=parse_dos,
                additional_fields=self.additional_fields,
                update_duplicates=self.update_duplicates)
            t_id, d = drone.assimilate(prev_dir, launches_coll=LaunchPad.auto_load().launches)

        mpsnl = d['snl_final'] if 'snl_final' in d else d['snl']
        snlgroup_id = d['snlgroup_id_final'] if 'snlgroup_id_final' in d else d['snlgroup_id']
        update_spec.update({'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id})

        print 'ENTERED task id:', t_id
        stored_data = {'task_id': t_id}
        if d['state'] == 'successful':
            update_spec['analysis'] = d['analysis']
            update_spec['output'] = d['output']
            return FWAction(stored_data=stored_data, update_spec=update_spec)

        # not successful - first test to see if UnconvergedHandler is needed
        if not fizzled_parent:
            unconverged_tag = 'unconverged_handler--{}'.format(fw_spec['prev_task_type'])
            output_dir = last_relax(os.path.join(prev_dir, 'vasprun.xml'))
            ueh = UnconvergedErrorHandler(output_filename=output_dir)
            if ueh.check() and unconverged_tag not in fw_spec['run_tags']:
                print 'Unconverged run! Creating dynamic FW...'

                spec = {'prev_vasp_dir': prev_dir,
                        'prev_task_type': fw_spec['task_type'],
                        'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id,
                        'task_type': fw_spec['prev_task_type'],
                        'run_tags': list(fw_spec['run_tags']),
                        'parameters': fw_spec.get('parameters'),
                        '_dupefinder': DupeFinderVasp().to_dict(),
                        '_priority': fw_spec['_priority']}

                snl = StructureNL.from_dict(spec['mpsnl'])
                spec['run_tags'].append(unconverged_tag)
                spec['_queueadapter'] = QA_VASP

                fws = []
                connections = {}

                f = Composition(
                    snl.structure.composition.reduced_formula).alphabetical_formula

                fws.append(Firework(
                    [VaspCopyTask({'files': ['INCAR', 'KPOINTS', 'POSCAR', 'POTCAR', 'CONTCAR'],
                                   'use_CONTCAR': False}), SetupUnconvergedHandlerTask(),
                     get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']),
                    fw_id=-2))

                spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True,
                        '_priority': fw_spec['_priority'], '_queueadapter': QA_DB,
                        'run_tags': list(fw_spec['run_tags'])}
                spec['run_tags'].append(unconverged_tag)
                fws.append(
                    Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                             fw_id=-1))
                connections[-2] = -1

                wf = Workflow(fws, connections)

                return FWAction(detours=wf)

        # not successful and not due to convergence problem - FIZZLE
        raise ValueError("DB insertion successful, but don't know how to fix this Firework! Can't continue with workflow...")
