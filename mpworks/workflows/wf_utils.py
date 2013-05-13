import glob
import os
import shutil


__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'


def j_decorate(m_dict):
    m_dict['auto_npar'] = False
    return m_dict


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
            '/scratch/scratchdirs/matcomp/', '/scratch2/scratchdirs/matcomp/',
            '/global/scratch/sd/matcomp/aj_tests/']

    for preamble in locs:
        new_loc = os.path.join(preamble, block_part)
        if os.path.exists(new_loc):
            return new_loc

    raise ValueError('get_loc() -- dir does not exist!!')


def move_to_garden(m_dir):
    block_part = get_block_part(m_dir)
    garden_part = '/project/projectdirs/matgen/garden/'
    f_dir = os.path.join(garden_part, block_part)
    shutil.move(m_dir, f_dir)
    return f_dir