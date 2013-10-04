import shlex
import socket
from custodian import Custodian
from custodian.vasp.jobs import VaspJob
from fireworks.core.firework import FireTaskBase
from fireworks.utilities.fw_serializers import FWSerializable
from pymatgen import PMGJSONDecoder

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Oct 03, 2013'

class CustodianTaskEx(FireTaskBase, FWSerializable):
    _fw_name = "Vasp Custodian Task"

    def __init__(self, parameters):
        self.update(parameters)
        # get VaspJob objects from 'jobs' parameter in FireWork
        self.jobs = map(VaspJob.from_dict, self['jobs'])
        # get VaspHandler objects from 'handlers' parameter in FireWork
        self.handlers = map(PMGJSONDecoder().process_decoded, self['handlers'])
        self.max_errors = self.get('max_errors', 1)

    def run_task(self, fw_spec):
        # Figure out the appropriate Vasp Executable
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
        custodian_out = c.run()

        update_spec = {'prev_vasp_dir': get_block_part(os.getcwd()),
                       'prev_task_type': fw_spec['task_type'],
                       'mpsnl': fw_spec['mpsnl'],
                       'snlgroup_id': fw_spec['snlgroup_id'],
                       'run_tags': fw_spec['run_tags']}

        return FWAction(stored_data=stored_data, update_spec=update_spec)

    def _write_formula_file(self, fw_spec):
        filename = get_slug(
            'JOB--' + fw_spec['mpsnl']['reduced_cell_formula_abc'] + '--'
            + fw_spec['task_type'])
        with open(filename, 'w+') as f:
            f.write('')