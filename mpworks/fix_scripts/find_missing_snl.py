import os
import traceback
from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymatgen.matproj.snl import is_valid_bibtex

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Oct 16, 2013'

# find SNL missing an SNLgroup

if __name__ == '__main__':

    module_dir = os.path.dirname(os.path.abspath(__file__))
    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)

    all_snl_ids = []  # snl ids that have a group
    all_missing_ids = []  # snl ids missing a group
    idx = 0
    print 'GETTING GROUPS'
    for x in snldb.snlgroups.find({}, {"all_snl_ids": 1}):
        all_snl_ids.extend(x['all_snl_ids'])

    print 'CHECKING SNL'
    for x in snldb.snl.find({}, {'snl_id': 1}, timeout=False):
        print x['snl_id']
        if x['snl_id'] not in all_snl_ids:
            print x['snl_id'], '*********'
            all_missing_ids.append(x['snl_id'])

    print 'FIXING / ADDING GROUPS'
    print all_missing_ids

    for snl_id in all_missing_ids:
        try:
            mpsnl = MPStructureNL.from_dict(snldb.snl.find_one({"snl_id": snl_id}))
            snldb.build_groups(mpsnl)
            print 'SUCCESSFUL', snl_id
        except:
            print 'ERROR with snl_id', snl_id
            traceback.print_exc()

