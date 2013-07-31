import os
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jul 30, 2013'

def get_icsd_ids_from_snlgroup(snlgroup_id, snldb):
    snl_ids = snldb.snlgroups.find_one({"snlgroup_id": snlgroup_id}, {"all_snl_ids":1})["all_snl_ids"]

    for snl in snldb.snl.find({"snl_id":{"$in": snl_ids}}, {"about._icsd.icsd_id": 1}):
        print snl["about"]["_icsd"]["icsd_id"]


if __name__ == '__main__':
    module_dir = os.path.dirname(os.path.abspath(__file__))
    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)
    print get_icsd_ids_from_snlgroup(22812, snldb)