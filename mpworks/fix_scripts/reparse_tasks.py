import json
import logging
import os
import sys
from pymongo import MongoClient, ASCENDING
from fireworks.core.launchpad import LaunchPad
from mpworks.drones.mp_vaspdrone import MPVaspDrone

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jun 13, 2013'


if __name__ == '__main__':
    # get the directory containing the db file
    db_dir = os.environ['DB_LOC']
    db_path = os.path.join(db_dir, 'tasks_db.json')

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('MPVaspDrone')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(getattr(logging, 'INFO'))
    logger.addHandler(sh)

    with open(db_path) as f:
        db_creds = json.load(f)
        conn = MongoClient(db_creds['host'], db_creds['port'])
        db = conn[db_creds['database']]
        db.authenticate(db_creds['admin_user'], db_creds['admin_password'])
        coll = db[db_creds['collection']]

        for d in coll.find({},{'dir_name_full': 1, 'task_id': 1, 'task_type': 1}, sort=[("task_id", ASCENDING)]):
            dir_name = d['dir_name_full']
            parse_dos = 'Uniform' in d['task_type']
            print 'REPARSING', d['task_id']
            drone = MPVaspDrone(
                host=db_creds['host'], port=db_creds['port'],
                database=db_creds['database'], user=db_creds['admin_user'],
                password=db_creds['admin_password'],
                collection=db_creds['collection'], parse_dos=parse_dos,
                additional_fields={},
                update_duplicates=True)
            t_id, d = drone.assimilate(dir_name, launches_coll=LaunchPad.auto_load().launches)