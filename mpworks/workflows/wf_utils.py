import glob
import os
import shutil
import time
import traceback
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
