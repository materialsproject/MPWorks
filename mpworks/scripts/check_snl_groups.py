"""
A runnable script to check all SNL groups
"""
__author__ = 'Patrick Huck'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Patrick Huck'
__email__ = 'phuck@lbl.gov'
__date__ = 'September 22, 2014'

import sys
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
    sm = StructureMatcher(
        ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
        attempt_supercell=False, comparator=ElementComparator()
    )

    # 0) check spacegroups of all available SNL's. This task can be split in
    #    multiple parallel jobs by SNL id ranges
    mpsnl_cursor = sma.snl.find({ "snl_id": id_range})
    for mpsnl_dict in mpsnl_cursor:
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        print 'snl_id = %d: %d %d' % (
            mpsnl_dict['snl_id'], mpsnl.sg_num, sf.get_spacegroup_number()
        )

    # 1) check whether every member of all_snl_ids in each snlgroup_id still
    #    matches canonical_snl_id. This task can be split in multiple parallel
    #    jobs by SNLGroup id ranges
    snlgrp_cursor = sma.snlgroups.find({ "snlgroup_id": id_range})
    for snlgrp_dict in snlgrp_cursor:
        snlgrp = SNLGroup.from_dict(snlgrp_dict)
        print snlgrp.all_snl_ids
        for snl_id in snlgrp.all_snl_ids[1:]: # first one is canonical id
            mpsnl_dict = sma.snl.find_one({ "snl_id": snl_id })
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            print 'snl_id = %d: %d' % (
                snl_id, sm.fit(mpsnl.structure, snlgrp.canonical_structure)
            )

    # 2) check whether canonical SNLs of two different groups match. This task
    #    can be split in multiple parallel jobs by SNLGroup combinations. Here,
    #    use artificial reduced test set of SNLGroup's.
    for id1 in range(args.start, args.end):
        snlgrp_dict1 = sma.snlgroups.find_one({ "snlgroup_id": id1 })
        struc1 = SNLGroup.from_dict(snlgrp_dict1).canonical_structure
        for id2 in range(id1+1, args.end):
            snlgrp_dict2 = sma.snlgroups.find_one({ "snlgroup_id": id2 })
            struc2 = SNLGroup.from_dict(snlgrp_dict2).canonical_structure
            match = sm.fit(struc1, struc2) # most return None due to different composition
            if match is not None:
                print 'snlgroup_ids = (%d,%d): %d' % (id1, id2, match)
            else:
                print('.'),
                sys.stdout.flush()

if __name__ == '__main__':
    check_snl_groups()
