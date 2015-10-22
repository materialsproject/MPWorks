import os

from pymongo import MongoClient, ASCENDING
import yaml

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 14, 2013'

if __name__ == '__main__':

    module_dir = os.path.dirname(os.path.abspath(__file__))
    # lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    tasks_f = os.path.join(module_dir, 'tasks.yaml')

    #with open(lp_f) as f:
    #    lp = LaunchPad.from_file(lp_f)
    #    lp.reset(None, require_password=False)

    with open(tasks_f) as f2:
        db_creds = yaml.load(f2)

        mc2 = MongoClient(db_creds['host'], db_creds['port'])
        db2 = mc2[db_creds['database']]
        db2.authenticate(db_creds['admin_user'], db_creds['admin_password'])
        new_tasks = db2['tasks']

    print new_tasks.count()

    new_tasks.ensure_index("task_id", unique=True)
    new_tasks.ensure_index("task_id_deprecated", unique=True)
    new_tasks.ensure_index("chemsys")
    new_tasks.ensure_index("analysis.e_above_hull")
    new_tasks.ensure_index("pretty_formula")
    new_tasks.ensure_index([("elements", ASCENDING), ("nelements", ASCENDING)])
    new_tasks.ensure_index("state")
    new_tasks.ensure_index([("state", ASCENDING), ("task_type", ASCENDING)])
    new_tasks.ensure_index([("state", ASCENDING), ("task_type", ASCENDING), ("submission_id", ASCENDING)])
    new_tasks.ensure_index("is_compatible")
    new_tasks.ensure_index("snl.snl_id")
    new_tasks.ensure_index("snlgroup_id")

    """
    for task_dict in new_tasks.find({"state":"successful"}, sort=[("task_id", ASCENDING)], timeout=False):
        fw_id = task_dict_to_wf(task_dict, lp)
        new_tasks.update({"task_id": task_dict["task_id"]}, {"$set": {"fw_id": fw_id}})
    """
