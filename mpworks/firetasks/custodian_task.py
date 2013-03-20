import socket
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.handlers import VaspErrorHandler, UnconvergedErrorHandler, PoscarErrorHandler
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

    _fw_name = "Custodian Task"

    def run_task(self, fw_spec):
        if 'nid' in socket.gethostname():  # hopper compute nodes
            v_exe = shlex.split('aprun -n 24 vasp')  # TODO: make ncores dynamic!
        elif 'c' in socket.gethostname():  # carver compute nodes
            v_exe = shlex.split('mpirun -n 8 vasp')  # TODO: make ncores dynamic!
        else:
            raise ValueError('Unrecognized host!')

        handlers = [VaspErrorHandler(), PoscarErrorHandler()]

        if 'static_run' in fw_spec['task_type']:
            jobs = VaspJob(v_exe,suffix=".static")  # TODO: fix this
        elif 'optimize structure (2x)' in fw_spec['task_type']:
            jobs = VaspJob.double_relaxation_run(v_exe)

        c = Custodian(handlers, jobs, max_errors=10)
        error_details = c.run()

        stored_data = {'error_details': error_details, 'error_names': error_details.keys()}

        return FWAction('CONTINUE', stored_data, {'$set': {'prev_VASP_dir': os.getcwd()}})
