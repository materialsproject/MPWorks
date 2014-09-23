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
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator, SpeciesComparator

def check_snl_groups():
    parser = ArgumentParser(description='program to check SNL groups')
    parser.add_argument('--start', help='start Id', default=1, type=int)
    parser.add_argument('--end', help='end Id', default=11, type=int)
    args = parser.parse_args()
    sma = SNLMongoAdapter.auto_load()
    id_range = {"$gte": args.start, "$lt": args.end}

    # 0) check spacegroups of all available SNL's
    mpsnl_dicts = sma.snl.find({ "snl_id": id_range})
    for mpsnl_dict in mpsnl_dicts:
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        print 'snl_id = %d: %d %d' % (
            mpsnl_dict['snl_id'], mpsnl.sg_num, sf.get_spacegroup_number()
        )

    # 1) check whether every member of all_snl_ids in each snlgroup_id still
    #    matches canonical_snl_id
    sm = StructureMatcher(
        ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
        attempt_supercell=False, comparator=ElementComparator()
    )
    snlgrp_dicts = sma.snlgroups.find({ "snlgroup_id": id_range})
    for snlgrp_dict in snlgrp_dicts:
        snlgrp = SNLGroup.from_dict(snlgrp_dict)
        print snlgrp.all_snl_ids
        for snl_id in snlgrp.all_snl_ids[1:]: # first one is canonical id
            mpsnl_dict = sma.snl.find_one({ "snl_id": snl_id })
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            print 'snl_id = %d: %d' % (
                snl_id, sm.fit(mpsnl.structure, snlgrp.canonical_structure)
            )

if __name__ == '__main__':
    check_snl_groups()
