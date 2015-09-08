import json
import logging
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
            update_duplicates=True)

    def process_task(self, task_id):
        t = self.old_tasks.find_one({'task_id': task_id})
        try:
            t_id, d = self.drone.assimilate(t)
            print 'ENTERED', t_id
        except:
            print 'ERROR entering', t['task_id']
            traceback.print_exc()


if __name__ == '__main__':
    o = OldTaskBuilder()
    o.setup()
    logging.basicConfig(level=logging.INFO)
    """
    tasks_old = OldTaskBuilder.old_tasks
    for i in tasks_old.find({'dir_name':{'$regex':'cathode_'}}, {'task_id': 1, 'dir_name': 1}):
        task_id = i['task_id']
        dir_name = i['dir_name']
        print 'FIXING', task_id
        # cut off the last part of the dir_name
        cutoff_path = os.path.dirname(dir_name)
        final_path = cutoff_path.replace('cathode_block', 'block')
        o.old_tasks.find_and_modify({'task_id': task_id}, {'$set': {'dir_name': final_path}})
        # o.process_task(task_id)
    """
    with open('to_fix.txt') as f:
        for line in f:
            old_task_id = int(line.split(' ')[1])
            new_task_id = 'mp-'+str(old_task_id)
            t = o.new_tasks.find_one({"task_id": new_task_id}, {"state": 1})
            if t:
                o.new_tasks.remove({'task_id': new_task_id})
                print 'REPARSING', old_task_id
                o.process_task(old_task_id)
