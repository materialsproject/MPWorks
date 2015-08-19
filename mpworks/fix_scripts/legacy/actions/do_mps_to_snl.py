import os
import traceback
import time

from pymongo import MongoClient
import yaml

from mpworks.fix_scripts.legacy.mps_to_snl import mps_dict_to_snl
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 08, 2013'


RESET = False

if __name__ == '__main__':

    module_dir = os.path.dirname(os.path.abspath(__file__))
    automation_f = os.path.join(module_dir, 'automation.yaml')
    snl_f = os.path.join(module_dir, 'snl.yaml')

    with open(automation_f) as f:
        y = yaml.load(f)

    mc = MongoClient(y['host'], y['port'])
    db = mc[y['db']]

    db.authenticate(y['username'], y['password'])

    snldb = SNLMongoAdapter.from_file(snl_f)

    prev_ids = []  # MPS ids that we already took care of

    print 'INITIALIZING'
    if RESET:
        snldb._reset()
        time.sleep(10)  # makes me sleep better at night

    else:
        for mps in snldb.snl.find({}, {"about._materialsproject.deprecated.mps_ids": 1}):
            prev_ids.extend(mps['about']['_materialsproject']['deprecated']['mps_ids'])

    print 'PROCESSING'
    for mps in db.mps.find(timeout=False):
        try:
            if not mps['mps_id'] in prev_ids:
                snl = mps_dict_to_snl(mps)
                if snl:
                    snldb.add_snl(snl)
            else:
                print 'SKIPPING', mps['mps_id']
        except:
            traceback.print_exc()
            print 'ERROR - mps id:', mps['mps_id']

    print 'DONE'
