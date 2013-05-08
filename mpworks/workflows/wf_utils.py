import glob
import os
from custodian.vasp.handlers import VaspErrorHandler, FrozenJobErrorHandler, MeshSymmetryErrorHandler, NonConvergingErrorHandler
from custodian.vasp.jobs import VaspJob
from mpworks.firetasks.custodian_task import VaspCustodianTask

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'


def _get_metadata(snl):
    md = {'run_tags': ['auto generation v1.0']}
    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        md['submission_id'] = snl.data['_materialsproject']['submission_id']

    return md


def _get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    if 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
    else:
        jobs = [VaspJob(v_exe)]

    handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(), NonConvergingErrorHandler()]
    params = {'jobs': [j.to_dict for j in jobs],
              'handlers': [h.to_dict for h in handlers], 'max_errors': 10, 'auto_npar': False,
              'auto_gamma': False}

    return VaspCustodianTask(params)


def last_relax(filename):
    if os.path.exists(filename):
        return filename
    relaxations = glob.glob('%s.relax*' % filename)
    if relaxations:
        return relaxations[-1]
    return filename


def orig(filename):
    orig = glob.glob('%s.orig' % filename)
    if orig:
        return orig[0]
    else:
        return filename