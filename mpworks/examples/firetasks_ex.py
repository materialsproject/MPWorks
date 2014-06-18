import json
import os
import shlex
import socket
from custodian import Custodian
from custodian.vasp.jobs import VaspJob
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.core.launchpad import LaunchPad
from fireworks.utilities.fw_serializers import FWSerializable
from matgendb.creator import VaspToDbTaskDrone
from mpworks.drones.mp_vaspdrone import MPVaspDrone
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
        nproc = os.environ['PBS_NP']

        # Figure out the appropriate Vasp Executable based on run machine
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n '+str(nproc)+' vasp')
            gv_exe = shlex.split('aprun -n '+str(nproc)+' gvasp')
            print 'running on HOPPER'
        elif 'c' in socket.gethostname():  # mendel compute nodes
            v_exe = shlex.split('mpirun -n '+str(nproc)+' vasp')
            gv_exe = shlex.split('mpirun -n '+str(nproc)+' gvasp')
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


    def run_task(self, fw_spec):
        prev_dir = fw_spec['prev_vasp_dir']

        # get the db credentials
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')

        # use MPDrone to put it in the database
        with open(db_path) as f:
            db_creds = json.load(f)
            drone = VaspToDbTaskDrone(
                host=db_creds['host'], port=db_creds['port'],
                database=db_creds['database'], user=db_creds['admin_user'],
                password=db_creds['admin_password'],
                collection=db_creds['collection'])
            t_id = drone.assimilate(prev_dir)

        if t_id:
            print 'ENTERED task id:', t_id
            stored_data = {'task_id': t_id}
            update_spec = {'prev_vasp_dir': prev_dir, 'prev_task_type': fw_spec['prev_task_type']}
            return FWAction(stored_data=stored_data, update_spec=update_spec)
        else:
            raise ValueError("Could not parse entry for database insertion!")
