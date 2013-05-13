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


def j_decorate(m_dict):
    m_dict['auto_npar'] = False
    return m_dict


def _get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    if 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
    else:
        jobs = [VaspJob(v_exe)]

    handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(),
                NonConvergingErrorHandler()]
    params = {'jobs': [j_decorate(j.to_dict) for j in jobs],
              'handlers': [h.to_dict for h in handlers], 'max_errors': 10}

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


def get_block_part(m_dir):
    return m_dir[m_dir.find('block_'):]


def get_loc(m_dir):
    if os.path.exists(m_dir):
        return m_dir
    block_part = get_block_part(m_dir)
    locs = ['/project/projectdirs/matgen/garden/', '/global/scratch/sd/matcomp/',
            '/scratch/scratchdirs/matcomp/', '/scratch2/scratchdirs/matcomp/']

    for preamble in locs:
        new_loc = os.path.join(preamble, block_part)
        if os.path.exists(new_loc):
            return new_loc

    raise ValueError('get_loc() -- dir does not exist!!')