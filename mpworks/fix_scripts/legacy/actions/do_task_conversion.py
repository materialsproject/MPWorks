from argparse import ArgumentParser
import json
import logging
import multiprocessing
import os
import traceback

from pymongo import MongoClient
import yaml

from mpworks.fix_scripts.legacy import MPVaspDrone_CONVERSION

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 13, 2013'

class OldTaskBuilder():

    @classmethod
    def setup(cls):
        module_dir = os.path.dirname(os.path.abspath(__file__))
        tasks_f = os.path.join(module_dir, 'mg_core_dev.yaml')

        with open(tasks_f) as f:
            y = yaml.load(f)

            mc = MongoClient(y['host'], y['port'])
            db = mc[y['db']]
            db.authenticate(y['username'], y['password'])

            cls.old_tasks = db['tasks_dbv2']

        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')
        with open(db_path) as f2:
            db_creds = json.load(f2)

            mc2 = MongoClient(db_creds['host'], db_creds['port'])
            db2 = mc2[db_creds['database']]
            db2.authenticate(db_creds['admin_user'], db_creds['admin_password'])

            cls.new_tasks = db2['tasks']

            cls.drone = MPVaspDrone_CONVERSION(
            host=db_creds['host'], port=db_creds['port'],
            database=db_creds['database'], user=db_creds['admin_user'],
            password=db_creds['admin_password'],
            collection=db_creds['collection'], parse_dos=False,
            additional_fields={},
            update_duplicates=False)

    def process_task(self, task_id):
        # get the directory containing the db file
        if not self.new_tasks.find_one({'task_id': 'mp-{}'.format(task_id)}):
            t = self.old_tasks.find_one({'task_id': task_id})
            try:
                t_id, d = self.drone.assimilate(t)
                print 'ENTERED', t_id
            except:
                print 'ERROR entering', t['task_id']
                traceback.print_exc()
        else:
            print 'skip'


def _analyze(task_id):
    b = OldTaskBuilder()
    return b.process_task(task_id)


def parallel_build(min, max):
    tasks_old = OldTaskBuilder.old_tasks
    task_ids = []
    for i in tasks_old.find({'task_id': {'$gte': min, '$lt': max}}, {'task_id': 1}):
        task_ids.append(i['task_id'])

    print 'GOT all tasks...'
    pool = multiprocessing.Pool(16)
    pool.map(_analyze, task_ids)
    print 'DONE'

if __name__ == '__main__':
    o = OldTaskBuilder()
    o.setup()
    logging.basicConfig(level=logging.INFO)
    parser = ArgumentParser()
    parser.add_argument('min', help='min', type=int)
    parser.add_argument('max', help='max', type=int)
    args = parser.parse_args()
    parallel_build(args.min, args.max)
