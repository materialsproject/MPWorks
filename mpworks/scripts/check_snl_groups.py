"""
A runnable script to check all SNL groups
"""
__author__ = 'Patrick Huck'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Patrick Huck'
__email__ = 'phuck@lbl.gov'
__date__ = 'September 22, 2014'

from argparse import ArgumentParser
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter

def check_snl_groups():
    parser = ArgumentParser(description='program to check SNL groups')
    parser.add_argument('--start', help='start SNL Id', default=0, type=int)
    parser.add_argument('--end', help='end SNL Id', default=50, type=int)
    args = parser.parse_args()

    sma = SNLMongoAdapter.auto_load()
    print sma

if __name__ == '__main__':
    check_snl_groups()
