import socket
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.handlers import VaspErrorHandler
from custodian.vasp.jobs import VaspJob
import shlex
import os

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class VaspCustodianTask(FireTaskBase, FWSerializable):
    _fw_name = "Vasp Custodian Task"

    def __init__(self, parameters):
        self.update(parameters)
        self.jobs = [VaspJob.from_dict(d) for d in self['jobs']]
        self.handlers = [VaspErrorHandler.from_dict(d)
                         for d in self['handlers']]
        self.max_errors = self.get('max_errors', 1)

    def run_task(self, fw_spec):
        # TODO: make this better - is there a way to load an environment variable as the VASP_EXE?
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n 48 vasp')  # TODO: make ncores dynamic!
        elif 'c' in socket.gethostname():  # carver / mendel compute nodes
            v_exe = shlex.split('mpirun -n 32 vasp')  # TODO: make ncores dynamic!
        else:
            raise ValueError('Unrecognized host!')

        for job in self.jobs:
            job.vasp_command = v_exe

        c = Custodian(self.handlers, self.jobs, self.max_errors)
        custodian_out = c.run()

        all_errors = set()
        for run in custodian_out:
            for correction in run['corrections']:
                all_errors.update(correction['errors'])

        stored_data = {'error_list': list(all_errors)}
        update_spec = {'prev_vasp_dir': os.getcwd(), 'prev_task_type': fw_spec['task_type']}

        if 'mpsnl' in fw_spec:
            update_spec.update({'mpsnl': fw_spec['mpsnl'], 'snlgroup_id': fw_spec['snlgroup_id']})

        return FWAction(stored_data=stored_data, update_spec=update_spec)