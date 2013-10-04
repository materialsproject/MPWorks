import os
import shlex
import socket
from custodian import Custodian
from custodian.vasp.jobs import VaspJob
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from pymatgen import PMGJSONDecoder

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Oct 03, 2013'

class VaspCustodianTaskEx(FireTaskBase, FWSerializable):
    _fw_name = "Vasp Custodian Task (Example)"

    def __init__(self, parameters):
        parameters = parameters if parameters else {}
        self.update(parameters)
        # get VaspJob objects from 'jobs' parameter in FireWork
        self.jobs = map(VaspJob.from_dict, parameters['jobs'])
        # get VaspHandler objects from 'handlers' parameter in FireWork
        self.handlers = map(PMGJSONDecoder().process_decoded, parameters['handlers'])
        self.max_errors = parameters['max_errors']

    def run_task(self, fw_spec):
        # Figure out the appropriate Vasp Executable based on run machine
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n 48 vasp')
            gv_exe = shlex.split('aprun -n 48 gvasp')
            print 'running on HOPPER'
        elif 'c' in socket.gethostname():  # mendel compute nodes
            v_exe = shlex.split('mpirun -n 32 vasp')
            gv_exe = shlex.split('mpirun -n 32 gvasp')
            print 'running on MENDEL'
        else:
            raise ValueError('Unrecognized host!')

        # override vasp executable in custodian jobs
        for job in self.jobs:
            job.vasp_cmd = v_exe
            job.gamma_vasp_cmd = gv_exe

        # run the custodian
        c = Custodian(self.handlers, self.jobs, self.max_errors)
        c.run()

        update_spec = {'prev_vasp_dir': os.getcwd(),
                       'prev_task_type': fw_spec['task_type']}

        return FWAction(update_spec=update_spec)

class VaspToDBTaskEx(FireTaskBase, FWSerializable):
    """
    Enter the VASP run directory in 'prev_vasp_dir' to the database.
    """

    _fw_name = "Vasp to Database Task (Example)"

    def __init__(self, parameters=None):
        """
        :param parameters: (dict) Potential keys are 'additional_fields', and 'update_duplicates'
        """
        parameters = parameters if parameters else {}
        self.update(parameters)

        self.additional_fields = self.get('additional_fields', {})
        self.update_duplicates = self.get('update_duplicates', False)

    def run_task(self, fw_spec):
            prev_dir = fw_spec['prev_vasp_dir']
            update_spec = {'prev_vasp_dir': prev_dir,
                           'prev_task_type': fw_spec['prev_task_type']}
            self.additional_fields['run_tags'] = fw_spec['run_tags']
            fizzled_parent = False
            parse_dos = 'Uniform' in fw_spec['prev_task_type']

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

                spec = {'prev_vasp_dir': get_block_part(prev_dir),
                        'prev_task_type': fw_spec['task_type'],
                        'mpsnl': mpsnl, 'snlgroup_id': snlgroup_id,
                        'task_type': fw_spec['prev_task_type'],
                        'run_tags': list(fw_spec['run_tags']),
                        '_dupefinder': DupeFinderVasp().to_dict(),
                        '_priority': fw_spec['_priority']}

                snl = StructureNL.from_dict(spec['mpsnl'])
                spec['run_tags'].append(unconverged_tag)
                spec['_queueadapter'] = QA_VASP

                fws = []
                connections = {}

                f = Composition.from_formula(
                    snl.structure.composition.reduced_formula).alphabetical_formula

                fws.append(FireWork(
                    [VaspCopyTask({'files': ['INCAR', 'KPOINTS', 'POSCAR', 'POTCAR', 'CONTCAR'],
                                   'use_CONTCAR': False}), SetupUnconvergedHandlerTask(),
                     get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']),
                    fw_id=-2))

                spec = {'task_type': 'VASP db insertion', '_allow_fizzled_parents': True,
                        '_priority': fw_spec['_priority'], '_queueadapter': QA_DB,
                        'run_tags': list(fw_spec['run_tags'])}
                spec['run_tags'].append(unconverged_tag)
                fws.append(
                    FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                             fw_id=-1))
                connections[-2] = -1

                wf = Workflow(fws, connections)

                return FWAction(detours=wf)

        # not successful and not due to convergence problem - FIZZLE
        raise ValueError("DB insertion successful, but don't know how to fix this FireWork! Can't continue with workflow...")