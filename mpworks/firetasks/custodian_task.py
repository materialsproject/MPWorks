import socket
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.handlers import VaspErrorHandler, PoscarErrorHandler
from custodian.vasp.jobs import VaspJob
import shlex
import os

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


class CustodianTask(FireTaskBase, FWSerializable):
    # TODO: deprecate and remove once confirmed that Wei is not using this...
    _fw_name = "Custodian Task"

    def run_task(self, fw_spec):
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n 24 vasp')  # TODO: make ncores dynamic!
        elif 'c' in socket.gethostname():  # carver / mendel compute nodes
            v_exe = shlex.split('mpirun -n 16 vasp')  # TODO: make ncores dynamic!
        else:
            raise ValueError('Unrecognized host!')

        handlers = [VaspErrorHandler(), PoscarErrorHandler()]

        if 'static' in fw_spec['task_type'] or 'DOS' in fw_spec['task_type']:
            jobs = [VaspJob(v_exe)]
        elif 'optimize structure (2x)' in fw_spec['task_type']:
            jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
        else:
            raise ValueError('Unrecognized task type! {}'.format(fw_spec['task_type']))

        c = Custodian(handlers, jobs, max_errors=10)
        error_details = c.run()
        stored_data = {'error_details': error_details}  # TODO: make this better, i.e. have all errors as list
        return FWAction('MODIFY', stored_data, {'dict_update': {'prev_vasp_dir': os.getcwd()}})


class VASPCustodianTask(FireTaskBase, FWSerializable):

    _fw_name = "VASP Custodian Task"

    def __init__(self, parameters):
        self.parameters = parameters
        self.jobs = [VaspJob.from_dict(d) for d in parameters['jobs']]
        self.handlers = [VaspErrorHandler.from_dict(d) for d in parameters['handlers']]
        self.max_errors = parameters['max_errors']

    def run_task(self, fw_spec):
        # TODO: make this better
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n 24 vasp')  # TODO: make ncores dynamic!
        elif 'c' in socket.gethostname():  # carver / mendel compute nodes
            v_exe = shlex.split('mpirun -n 16 vasp')  # TODO: make ncores dynamic!
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
        
        return FWAction('MODIFY', stored_data, {'dict_update': {'prev_vasp_dir': os.getcwd()}})