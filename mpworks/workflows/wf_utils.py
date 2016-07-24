import glob
import logging
import os
import shlex
import shutil
import time
import traceback

import subprocess

import re
from monty.os.path import zpath
from mpworks.workflows.wf_settings import RUN_LOCS, GARDEN


__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'


NO_POTCARS = ['Po', 'At', 'Rn', 'Fr', 'Ra', 'Am', 'Cm', 'Bk', 'Cf', 'Es', 'Fm', 'Md', 'No', 'Lr']


def j_decorate(m_dict):
    m_dict['auto_npar'] = False
    return m_dict


def last_relax(filename):
    # for old runs
    m_dir = os.path.dirname(filename)
    m_file = os.path.basename(filename)

    if os.path.exists(zpath(os.path.join(m_dir, 'relax2', m_file))):
        return zpath(os.path.join(m_dir, 'relax2', m_file))

    elif os.path.exists(zpath(filename)):
        return zpath(filename)

    relaxations = glob.glob('%s.relax*' % filename)
    if relaxations:
        return sorted(relaxations)[-1]

    # backup for old runs
    elif os.path.exists(zpath(os.path.join(m_dir, 'relax1', m_file))):
        return zpath(os.path.join(m_dir, 'relax1', m_file))

    return filename


def orig(filename):
    orig = glob.glob('%s.orig' % filename)
    if orig:
        return orig[0]
    else:
        return filename


def get_block_part(m_dir):
    if 'block_' in m_dir:
        return m_dir[m_dir.find('block_'):]
    return m_dir


def get_loc(m_dir):
    if os.path.exists(m_dir):
        return m_dir
    block_part = get_block_part(m_dir)

    for preamble in RUN_LOCS:
        new_loc = os.path.join(preamble, block_part)
        if os.path.exists(new_loc):
            return new_loc

    raise ValueError('get_loc() -- dir does not exist!! Make sure your base directory is listed in RUN_LOCS of wf_settings.py')


def move_to_garden(m_dir, prod=False):
    block_part = get_block_part(m_dir)
    garden_part = GARDEN if prod else GARDEN+'/dev'
    f_dir = os.path.join(garden_part, block_part)
    if os.path.exists(m_dir) and not os.path.exists(f_dir) and m_dir != f_dir:
        try:
            shutil.move(m_dir, f_dir)
            time.sleep(30)
        except:
            # double check the move error is not due to path existing
            # there is sometimes a race condition with duplicate check
            if os.path.exists(f_dir):
                return f_dir
            traceback.print_exc()
            raise ValueError('Could not move file to GARDEN! {}'.format(traceback.format_exc()))


    return f_dir

class ScancelJobStepTerminator:
    """
    A tool to cancel a job step in a SLURM srun job using scancel command.
    """

    def __init__(self, stderr_filename):
        """

        Args:
            stderr_filename: The file name of the stderr for srun job step.
        """
        self.stderr_filename = stderr_filename

    def cancel_job_step(self):
        step_id = self.parse_srun_step_number()
        scancel_cmd = shlex.split("scancel --signal=KILL {}".format(step_id))
        logging.info("Terminate the job step using {}".format(' '.join(scancel_cmd)))
        subprocess.Popen(scancel_cmd)

    def parse_srun_step_number(self):
        step_pat_text = r"srun: launching (?P<step_id>\d+[.]\d+) on host \w+, \d+ tasks:"
        step_pat = re.compile(step_pat_text)
        step_id = None
        with open(self.stderr_filename) as f:
            err_text = f.readlines()
        for line in err_text:
            m = step_pat.search(line)
            if m is not None:
                step_id = m.group("step_id")
        if step_id is None:
            raise ValueError("Can't find SRUN job step number in STDERR file")
        return step_id
