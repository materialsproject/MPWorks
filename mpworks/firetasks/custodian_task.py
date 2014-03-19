import logging
import socket
from monty.os.path import which
from custodian.vasp.handlers import VaspErrorHandler, NonConvergingErrorHandler, \
    FrozenJobErrorHandler, MeshSymmetryErrorHandler
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from custodian.custodian import Custodian
from custodian.vasp.jobs import VaspJob
import shlex
import os
from fireworks.utilities.fw_utilities import get_slug
from mpworks.workflows.wf_utils import j_decorate
from pymatgen.serializers.json_coders import PMGJSONDecoder

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
        self.jobs = map(VaspJob.from_dict, self['jobs'])
        dec = PMGJSONDecoder()
        self.handlers = map(dec.process_decoded, self['handlers'])
        self.max_errors = self.get('max_errors', 1)
        self.gzip_output = self.get('gzip_output', True)

    def run_task(self, fw_spec):

        # write a file containing the formula and task_type for somewhat
        # easier file system browsing
        self._write_formula_file(fw_spec)

        if which("mpirun"):
            mpi_cmd = "mpirun"
        elif which("aprun"):
            mpi_cmd = "aprun"
        else:
            raise ValueError("No MPI command found!")

        nproc = os.environ['PBS_NP']

        v_exe = shlex.split('{} -n {} vasp'.format(mpi_cmd, nproc))
        gv_exe = shlex.split('{} -n {} gvasp'.format(mpi_cmd, nproc))

        print 'host:', os.environ['HOSTNAME']
        print v_exe
        print gv_exe

        for job in self.jobs:
            job.vasp_cmd = v_exe
            job.gamma_vasp_cmd = gv_exe

        logging.basicConfig(level=logging.DEBUG)
        c = Custodian(self.handlers, self.jobs, self.max_errors, gzipped_output=self.gzip_output)
        custodian_out = c.run()

        all_errors = set()
        for run in custodian_out:
            for correction in run['corrections']:
                all_errors.update(correction['errors'])

        stored_data = {'error_list': list(all_errors)}
        update_spec = {'prev_vasp_dir': os.getcwd(),
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


def get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    if 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
        handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(),
                    NonConvergingErrorHandler()]
    elif 'static' in task_type:
        jobs = [VaspJob(v_exe)]
        handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(),
                    NonConvergingErrorHandler()]
    else:
        # non-SCF runs
        jobs = [VaspJob(v_exe)]
        handlers = []

    params = {'jobs': [j_decorate(j.to_dict) for j in jobs],
              'handlers': [h.to_dict for h in handlers], 'max_errors': 5}

    return VaspCustodianTask(params)