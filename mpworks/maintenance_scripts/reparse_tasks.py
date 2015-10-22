from argparse import ArgumentParser
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
This script re-runs the MPVaspDrone over all the *new-style* tasks. It can be used when the MPVaspDrone is updated.

A few notes:
* The old-style tasks will be unaffected by this script
* The dos_fs and band_structure_fs collections should be completely deleted before running this script over the database.

Note - AJ has not run this code since its inception in May 2013. Changes may be needed.
'''

class TaskBuilder():

    @classmethod
    def setup(cls):
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')
        with open(db_path) as f2:
            db_creds = json.load(f2)
            mc2 = MongoClient(db_creds['host'], db_creds['port'])
            db2 = mc2[db_creds['database']]
            db2.authenticate(db_creds['admin_user'], db_creds['admin_password'])

            cls.tasks = db2['tasks']
            cls.host = db_creds['host']
            cls.port = db_creds['port']
            cls.database = db_creds['database']
            cls.collection = db_creds['collection']
            cls.admin_user = db_creds['admin_user']
            cls.admin_password = db_creds['admin_password']

    def process_task(self, data):

        try:
            dir_name = data[0]
            parse_dos = data[1]
            prev_info = self.tasks.find_one({'dir_name_full': dir_name}, {'task_type': 1, 'snl_final': 1, 'snlgroup_id_final': 1, 'snlgroup_changed': 1})
            drone = MPVaspDrone(
                host=self.host, port=self.port,
                database=self.database, user=self.admin_user,
                password=self.admin_password,
                collection=self.collection, parse_dos=parse_dos,
                additional_fields={},
                update_duplicates=True)
            t_id, d = drone.assimilate(dir_name, launches_coll=LaunchPad.auto_load().launches)


            self.tasks.update({"task_id": t_id}, {"$set": {"snl_final": prev_info['snl_final'], "snlgroup_id_final": prev_info['snlgroup_id_final'], "snlgroup_changed": prev_info['snlgroup_changed']}})
            print 'FINISHED', t_id
        except:
            print '-----'
            print 'ENCOUNTERED AN EXCEPTION!!!', data[0]
            traceback.print_exc()
            print '-----'


def _analyze(data):
    b = TaskBuilder()
    return b.process_task(data)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('MPVaspDrone')
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(getattr(logging, 'INFO'))
    logger.addHandler(sh)

    finished_tasks = []
    module_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(os.path.join(module_dir, 'finished_tasks.txt')):
        with open(os.path.join(module_dir, 'finished_tasks.txt')) as f:
            for line in f:
                task = line.split()[1].strip()
                finished_tasks.append(task)

    o = TaskBuilder()
    o.setup()
    tasks = TaskBuilder.tasks
    m_data = []
    # q = {'submission_id': {'$exists': False}}  # these are all new-style tasks
    #q = {"task_type":{"$regex":"band structure"}, "state":"successful", "calculations.0.band_structure_fs_id":{"$exists":False}}

    parser = ArgumentParser()
    parser.add_argument('min', help='min', type=int)
    parser.add_argument('max', help='max', type=int)
    args = parser.parse_args()
    q = {"task_id_deprecated": {"$lte": args.max, "$gte":args.min}, "is_deprecated": True}

    for d in tasks.find(q, {'dir_name_full': 1, 'task_type': 1, 'task_id': 1}, timeout=False):
        if d['task_id'] in finished_tasks:
            print 'DUPLICATE', d['task_id']
        else:
            o.process_task((d['dir_name_full'], 'Uniform' in d['task_type']))
            # m_data.append((d['dir_name_full'], 'Uniform' in d['task_type']))
    print 'DONE'