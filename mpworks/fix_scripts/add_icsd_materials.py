import os
import yaml
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jul 30, 2013'

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jul 16, 2013'

import json
import logging
import os
import sys
from pymongo import MongoClient
from fireworks.core.launchpad import LaunchPad
from mpworks.drones.mp_vaspdrone import MPVaspDrone
import multiprocessing
import traceback

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 13, 2013'

'''
This script updates the ICSD id of all materials
'''

class ICSDBuilder():

    @classmethod
    def setup(cls):
        module_dir = os.path.dirname(os.path.abspath(__file__))
        snl_f = os.path.join(module_dir, 'snl.yaml')
        cls.snldb = SNLMongoAdapter.from_file(snl_f)

        tasks_f = os.path.join(module_dir, 'materials.yaml')
        with open(tasks_f) as f2:
            task_creds = yaml.load(f2)

        mc = MongoClient(task_creds['host'], task_creds['port'])
        db = mc[task_creds['database']]
        db.authenticate(task_creds['admin_user'], task_creds['admin_password'])
        cls.materials = db[task_creds['collection']]

    def process_material(self, material_id):

        try:
            d = self.materials.find_one({"task_ids": material_id}, {"snlgroup_id_final": 1})
            snlgroup_id = d['snlgroup_id_final']
            icsd_ids = self.get_icsd_ids_from_snlgroup(snlgroup_id)

            self.materials.find_and_modify({"task_ids": material_id}, {"$set": {"icsd_id": icsd_ids}})
            print material_id, icsd_ids
            print 'FINISHED', material_id
        except:
            print '-----'
            print 'ENCOUNTERED AN EXCEPTION!!!', material_id
            traceback.print_exc()
            print '-----'


    def get_icsd_ids_from_snlgroup(self, snlgroup_id):
        snl_ids = self.snldb.snlgroups.find_one({"snlgroup_id": snlgroup_id}, {"all_snl_ids":1})["all_snl_ids"]

        icsd_ids = set()
        for snl in self.snldb.snl.find({"snl_id":{"$in": snl_ids}}, {"about._icsd.icsd_id": 1}):
            if '_icsd' in snl["about"] and snl["about"]["_icsd"].get("icsd_id"):
                icsd_ids.add(snl["about"]["_icsd"]["icsd_id"])

        return list(icsd_ids)


def _analyze(data):
    b = ICSDBuilder()
    return b.process_material(data)

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    o = ICSDBuilder()
    o.setup()
    materials = o.materials
    print materials.count()
    m_data = []
    for d in materials.find({}, {'task_id': 1}, timeout=False):
        m_data.append(d['task_id'])

    pool = multiprocessing.Pool(8)
    pool.map(_analyze, m_data)
    print 'DONE'
