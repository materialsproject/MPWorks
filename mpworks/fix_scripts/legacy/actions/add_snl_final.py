import os
from pymongo import MongoClient
import yaml

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 21, 2013'

module_dir = os.path.dirname(os.path.abspath(__file__))
tasks_f = os.path.join(module_dir, 'tasks.yaml')

with open(tasks_f) as f2:
    db_creds = yaml.load(f2)

    mc2 = MongoClient(db_creds['host'], db_creds['port'])
    db2 = mc2[db_creds['database']]
    db2.authenticate(db_creds['admin_user'], db_creds['admin_password'])
    new_tasks = db2['tasks']

    count = 0
    for d in new_tasks.find({'snlgroup_id_final': {'$exists': False}}, {'task_id': 1, 'snl': 1, 'snlgroup_id': 1, 'snlgroup_changed': 1}):
        new_tasks.update({'task_id': d['task_id']}, {'$set': {'snl_final': d['snl'], 'snlgroup_id_final': d['snlgroup_id'], 'snlgroup_changed': False}})
        count+=1
        print count
