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
This script re-runs the MPVaspDrone over all the tasks and just enters the deprecated task_id (as int).
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

    def process_task(self, task_id):

        try:
            task_id_deprecated = int(task_id.split('-')[-1])
            self.tasks.update({"task_id": task_id}, {"$set": {"task_id_deprecated": task_id_deprecated}})
            print 'FINISHED', task_id
        except:
            print '-----'
            print 'ENCOUNTERED AN EXCEPTION!!!', task_id
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

    o = TaskBuilder()
    o.setup()
    tasks = TaskBuilder.tasks
    m_data = []
    q = {}
    for d in tasks.find(q, {'task_id': 1}, timeout=False).limit(1):
        o.process_task(d['task_id'])
    print 'DONE'