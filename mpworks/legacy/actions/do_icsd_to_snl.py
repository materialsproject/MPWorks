import json
import os
import traceback
from pymongo import MongoClient
import yaml
from mpworks.legacy.icsd2012_to_snl import icsd_dict_to_snl
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import DATETIME_HANDLER

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 12, 2013'

if __name__ == '__main__':

    module_dir = os.path.dirname(os.path.abspath(__file__))
    icsd_f = os.path.join(module_dir, 'icsd2012.yaml')
    snl_f = os.path.join(module_dir, 'snl.yaml')

    with open(icsd_f) as f:
        y = yaml.load(f)

    mc = MongoClient(y['host'], y['port'])
    db = mc[y['db']]

    db.authenticate(y['username'], y['password'])

    # snldb = SNLMongoAdapter.from_file(snl_f)

    for icsd_dict in db.icsd_2012_crystals.find(timeout=False):
        try:
                snl = icsd_dict_to_snl(icsd_dict)
                print json.dumps(snl.to_dict, default=DATETIME_HANDLER)
                #if snl:
                #    snldb.add_snl(snl)
        except:
            traceback.print_exc()
            print 'ERROR - icsd id:', icsd_dict['icsd_id']

    print 'DONE'