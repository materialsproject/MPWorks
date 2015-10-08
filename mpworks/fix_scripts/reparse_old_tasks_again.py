from monty.os.path import zpath
from pymatgen.io.vasp import Outcar

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jun 13, 2013'

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
This script add selected information for all the *old-style* tasks.

A few notes:
* The new-style tasks will be unaffected by this script

'''

class OldTaskFixer():

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

    def process_task(self, path):
        try:
            #Override incorrect outcar subdocs for two step relaxations
            if os.path.exists(os.path.join(path, "relax2")):
                try:
                    run_stats = {}
                    for i in [1,2]:
                        outcar = Outcar(zpath(os.path.join(path,"relax"+str(i), "OUTCAR")))
                        m_key = "calculations."+str(i-1)+".output.outcar"
                        self.tasks.update({'dir_name_full': path}, {'$set': {m_key: outcar.as_dict()}})
                        run_stats["relax"+str(i)] = outcar.run_stats
                except:
                    logger.error("Bad OUTCAR for {}.".format(path))

                try:
                    overall_run_stats = {}
                    for key in ["Total CPU time used (sec)", "User time (sec)",
                                "System time (sec)", "Elapsed time (sec)"]:
                        overall_run_stats[key] = sum([v[key]
                                          for v in run_stats.values()])
                    run_stats["overall"] = overall_run_stats
                except:
                    logger.error("Bad run stats for {}.".format(path))

                self.tasks.update({'dir_name_full': path}, {'$set': {"run_stats": run_stats}})
                print 'FINISHED', path
            else:
                print 'SKIPPING', path
        except:
            print '-----'
            print 'ENCOUNTERED AN EXCEPTION!!!', path
            traceback.print_exc()
            print '-----'


def _analyze(data):
    b = OldTaskFixer()
    return b.process_task(data)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('MPVaspDrone')
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(getattr(logging, 'INFO'))
    logger.addHandler(sh)

    o = OldTaskFixer()
    o.setup()
    tasks = OldTaskFixer.tasks
    m_data = []
    with open('old_tasks.txt') as f:
        for line in f:
            old_task = line.split(' ')[1].strip()
            m_data.append(tasks.find_one({"task_id":old_task}, {'dir_name_full': 1})["dir_name_full"])
    print 'GOT all tasks...'
    # print len(m_data)
    # print m_data[1]
    pool = multiprocessing.Pool(2)
    pool.map(_analyze, m_data)
    print 'DONE'
