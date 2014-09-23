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
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.symmetry.finder import SymmetryFinder

def check_snl_groups():
    parser = ArgumentParser(description='program to check SNL groups')
    parser.add_argument('--start', help='start SNL Id', default=1, type=int)
    parser.add_argument('--end', help='end SNL Id', default=11, type=int)
    args = parser.parse_args()
    sma = SNLMongoAdapter.auto_load()

    # 0) check whether spacegroups of all available SNL's
    mpsnl_dicts = sma.snl.find({ "snl_id": {"$gte": args.start, "$lt": args.end}})
    for mpsnl_dict in mpsnl_dicts:
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        print mpsnl.sg_num, sf.get_spacegroup_number()

if __name__ == '__main__':
    check_snl_groups()
