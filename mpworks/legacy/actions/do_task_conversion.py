from argparse import ArgumentParser
import json
import logging
import multiprocessing
import os
import traceback
from pymongo import MongoClient, ASCENDING
import yaml
from mpworks.legacy.old_task_drone import MPVaspDrone_CONVERSION

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 13, 2013'


def get_old_tasks():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    tasks_f = os.path.join(module_dir, 'mg_core_dev.yaml')

    with open(tasks_f) as f:
        y = yaml.load(f)

        mc = MongoClient(y['host'], y['port'])
        db = mc[y['db']]
        db.authenticate(y['username'], y['password'])

        return db['tasks_dbv2']


def process_task(task_id):

        tasks_old = get_old_tasks()

        # get the directory containing the db file
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'tasks_db.json')
        logging.basicConfig(level=logging.INFO)
        with open(db_path) as f:
            db_creds = json.load(f)
            drone = MPVaspDrone_CONVERSION(
            host=db_creds['host'], port=db_creds['port'],
            database=db_creds['database'], user=db_creds['admin_user'],
            password=db_creds['admin_password'],
            collection=db_creds['collection'], parse_dos=False,
            additional_fields={},
            update_duplicates=False)
            t = tasks_old.find_one({'task_id': task_id})
            if t:
                # get the directory containing the db file
                try:
                    t_id, d = drone.assimilate(t)
                    print 'ENTERED', t_id
                except:
                    print 'ERROR entering', t['task_id']
                    traceback.print_exc()


def parallel_build():

    tasks_old = get_old_tasks()
    task_ids = []
    for i in tasks_old.find({}, {'task_id': 1}):
        task_ids.append(i['task_id'])

    print 'GOT all tasks...'
    pool = multiprocessing.Pool(16)
    pool.map(process_task, task_ids)
    print 'DONE'

if __name__ == '__main__':
    #parser = ArgumentParser()
    #parser.add_argument('min', help='min', type=int)
    #parser.add_argument('max', help='max', type=int)
    #args = parser.parse_args()
    #parallel_build(args.min, args.max)
    parallel_build()