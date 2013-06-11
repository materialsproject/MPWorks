import glob
import os
from mpworks.workflows.wf_utils import get_block_part

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 29, 2013'

"""
The purpose of this script is to detect whether any directories were only partially moved from $SCRATCH dirs to $GARDEN due to a disk space error.
If it detects a directory that is in BOTH $SCRATCH and $GARDEN, it prints it.

Currently it is up to the user to manually move directories (for safety)
"""


SCRATCH_PATH = '/global/scratch/sd/matcomp'
GARDEN_PATH = '/project/projectdirs/matgen/garden/'


def detect():
    for d in glob.glob(os.path.join(SCRATCH_PATH, 'block*/launch*')):
        block_part = get_block_part(d)
        garden_dir = os.path.join(GARDEN_PATH, block_part)
        if os.path.exists(garden_dir):
            print garden_dir


if __name__ == '__main__':
    detect()