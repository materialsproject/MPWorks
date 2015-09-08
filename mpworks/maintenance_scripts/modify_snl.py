from collections import namedtuple
import os
from pymongo import MongoClient
import yaml
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from pymatgen.matproj.snl import StructureNL
import datetime

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Feb 20, 2014'


"""
This is used to modify SNL data. An SNL should not just be changed in the SNL collection because
that SNL is referred to in many different databases.

This code tries to properly update all relevant databases with the SNL
changes, not just the basic SNL collection.

Note that the lattice and sites of an SNL cannot be changed! This would be a different material
altogether and have affected the runs / duplicate checking.
"""


module_dir = os.path.dirname(os.path.abspath(__file__))
snl_f = os.path.join(module_dir, 'snl.yaml')
fw_f = os.path.join(module_dir, 'my_launchpad.yaml')
tasks_f = os.path.join(module_dir, 'tasks.yaml')

def get_colls():
    colls = namedtuple('Collections', ['snl', 'snlgroups'])
    sma = SNLMongoAdapter.from_file(snl_f)
    lp = LaunchPad.from_file(fw_f)

    colls.snl = sma.snl
    colls.snlgroups = sma.snlgroups
    colls.fireworks = lp.fireworks
    colls.launches = lp.launches

    with open(tasks_f) as f2:
        task_creds = yaml.load(f2)

    mc = MongoClient(task_creds['host'], task_creds['port'])
    db = mc[task_creds['database']]
    db.authenticate(task_creds['admin_user'], task_creds['admin_password'])
    colls.tasks = db['tasks']

    return colls


def modify_snl(snl_id, new_snl, colls, reject_bad_tasks=False):
    # get the old SNL lattice and sites
    snl_old = colls.snl.find_one({'snl_id': snl_id}, {'lattice': 1, 'sites': 1, 'snl_timestamp': 1})

    # enforce the new SNL's lattice/sites to be same as old
    snl_d = new_snl.as_dict()
    snl_d['lattice'] = snl_old['lattice']
    snl_d['sites'] = snl_old['sites']
    snl_d['snl_timestamp'] = snl_old['snl_timestamp']

    # insert the new SNL into the snl collection
    print 'INSERTING SNL_ID', {'snl_id': snl_id}, snl_d
    colls.snl.update({'snl_id': snl_id}, snl_d)

    # update the canonical SNL of the group
    for s in colls.snlgroups.find({'canonical_snl.about._materialsproject.snl_id': snl_id}, {'snlgroup_id': 1}):
        print 'CHANGING SNLGROUP_ID', s['snlgroup_id']
        colls.snlgroups.find_and_modify({'snlgroup_id': s['snlgroup_id']}, {'$set': {'canonical_snl': snl_d}})

    # update FWs pt 1
    for f in colls.fireworks.find({'spec.mpsnl.about._materialsproject.snl_id': snl_id}, {'fw_id': 1}):
        print 'CHANGING FW_ID', f['fw_id']
        colls.fireworks.find_and_modify({'fw_id': f['fw_id']}, {'$set': {'spec.mpsnl': snl_d}})

    # update FWs pt 2
    for f in colls.fireworks.find({'spec.force_mpsnl.about._materialsproject.snl_id': snl_id}, {'fw_id': 1}):
        print 'CHANGING FW_ID', f['fw_id']
        colls.fireworks.find_and_modify({'fw_id': f['fw_id']}, {'$set': {'spec.force_mpsnl': snl_d}})

    # update Launches
    for l in colls.launches.find({'action.update_spec.mpsnl.about._materialsproject.snl_id': snl_id}, {'launch_id': 1}):
        print 'CHANGING LAUNCH_ID', l['launch_id']
        colls.launches.find_and_modify({'launch_id': l['launch_id']}, {'$set': {'action.update_spec.mpsnl': snl_d}})

    # update tasks initial
    for t in colls.tasks.find({'snl.about._materialsproject.snl_id': snl_id}, {'task_id': 1}):
        print 'CHANGING init TASK_ID', t['task_id']
        colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$set': {'snl': snl_d}})
        if reject_bad_tasks:
            print 'REJECTING TASK_ID', t['task_id']
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$set': {'state': 'rejected'}})
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$push': {'analysis.errors_MP.critical_signals': 'BAD STRUCTURE SNL'}})
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$inc': {'analysis.errors_MP.num_critical': 1}})


    # update tasks final
    for t in colls.tasks.find({'snl_final.about._materialsproject.snl_id': snl_id}, {'task_id': 1}):
        print 'CHANGING final TASK_ID', t['task_id']
        colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$set': {'snl_final': snl_d}})
        if reject_bad_tasks:
            print 'REJECTING TASK_ID', t['task_id']
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$set': {'state': 'rejected'}})
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$push': {'analysis.errors_MP.critical_signals': 'BAD STRUCTURE SNL'}})
            colls.tasks.find_and_modify({'task_id': t['task_id']}, {'$inc': {'analysis.errors_MP.num_critical': 1}})

    # note: for now we are not fixing submissions in order to keep a record of submissions accurate, and also because the SNL assignment comes after submission

    print 'DONE PROCESSING', snl_id


def get_deprecated_snl(snl_id, colls):
    snl_old = colls.snl.find_one({'snl_id': snl_id})
    del snl_old['about']['_icsd']
    snl_old['about']['remarks'].append('Record updated (about._icsd deleted) {}'.format(datetime.datetime.now().strftime('%Y-%m-%d')))
    return MPStructureNL.from_dict(snl_old)


if __name__ == '__main__':

    colls = get_colls()
    snl_id = 1579

    snl_new = get_deprecated_snl(snl_id, colls)
    print snl_new.as_dict()

    modify_snl(snl_id, snl_new, colls, reject_bad_tasks=True)