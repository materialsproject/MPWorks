import os
from pymongo import MongoClient
import yaml
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.submission.submission_mongo import SubmissionMongoAdapter

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jun 05, 2013'


"""
The purpose of this script is to detect instances where we have bad structures for ICSD 2007. It looks like back in the 'old days', some crystals were not converted correctly when migrating from the 'crystals' collection to the 'refactored crystals' collection. That bug was carried over to the MPS collection, and in turn the SNL collection.

This script tries to detect these 'bad crystals' by cross-referencing with the new  ICSD 2012 import, which was done much more cleanly. If an ICSD id from 2012 is present in multiple SNLgroups, we want to remove the old entry as it is probably incorrect.
"""

def detect():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)

    snl = snldb.snl
    snlgroups = snldb.snlgroups
    q = {"about._icsd.icsd_id":{"$exists":True}}  # icsd strctures
    q["about._icsd.coll_code"] =  {"$exists":False} # old ICSD structure
    q["about.history.description.fw_id"] = {"$exists":False} # non structure relaxations

    for old_s in snl.find(q, {"snl_id": 1, 'about._icsd.icsd_id': 1, 'about._materialsproject.deprecated.crystal_id_deprecated': 1}):
        icsd_id = old_s['about']['_icsd']['icsd_id']
        crystal_id = old_s['about']['_materialsproject']['deprecated']['crystal_id_deprecated']

        new_s = snl.find_one({"about._icsd.icsd_id":icsd_id, "about._icsd.coll_code":{"$exists":True}}, {"snl_id": 1})
        if new_s:
            n_groups = snlgroups.find({"all_snl_ids":{"$in":[old_s['snl_id'], new_s['snl_id']]}}).count()
            if n_groups != 1:
                # The crystal_id is bad
                print crystal_id


def fix():

    # initialize databases
    module_dir = os.path.dirname(os.path.abspath(__file__))

    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)
    snl = snldb.snl
    snlgroups = snldb.snlgroups

    tasks_f = os.path.join(module_dir, 'tasks.yaml')
    with open(tasks_f) as f2:
        task_creds = yaml.load(f2)

    mc = MongoClient(task_creds['host'], task_creds['port'])
    db = mc[task_creds['database']]
    db.authenticate(task_creds['admin_user'], task_creds['admin_password'])
    tasks = db['tasks']

    tasks_f = os.path.join(module_dir, 'tasks.yaml')
    with open(tasks_f) as f2:
        task_creds = yaml.load(f2)

    mc = MongoClient(task_creds['host'], task_creds['port'])
    db = mc[task_creds['database']]
    db.authenticate(task_creds['admin_user'], task_creds['admin_password'])
    tasks = db['tasks']

    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)
    fws = lpdb.fireworks
    launches = lpdb.launches

    sb_f = os.path.join(module_dir, 'submission.yaml')
    sbdb = SubmissionMongoAdapter.from_file(sb_f)
    submissions = sbdb.jobs

    bad_crystal_ids = []

    crystals_file = os.path.join(module_dir, 'bad_crystals.txt')
    with open(crystals_file) as f:
        for line in f:
            bad_crystal_ids.append(int(line.strip()))


    for c_id in bad_crystal_ids:
        if c_id == 100892 or c_id == 100202:
            print 'SKIP'

        else:
            # FIX SNL
            for s in snl.find({'about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'snl_id': 1}):
                snl.update({'snl_id': s['snl_id']}, {'$pushAll': {"about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX SNLGROUPS
            for s in snlgroups.find({'canonical_snl.about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'snlgroup_id': 1}):
                snlgroups.update({'snlgroup_id': s['snlgroup_id']}, {'$pushAll': {"canonical_snl.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX FWs pt 1
            for s in fws.find({'spec.mpsnl.about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'fw_id': 1}):
                fws.update({'fw_id': s['fw_id']}, {'$pushAll': {"spec.mpsnl.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX FWs pt 2
            for s in fws.find({'spec.force_mpsnl.about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'fw_id': 1}):
                fws.update({'fw_id': s['fw_id']}, {'$pushAll': {"spec.force_mpsnl.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX Launches
            for s in launches.find({'action.update_spec.mpsnl.about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'launch_id': 1}):
                launches.update({'launch_id': s['launch_id']}, {'$pushAll': {"action.update_spec.mpsnl.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX TASKS
            for s in tasks.find({'snl.about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'task_id': 1}):
                tasks.update({'task_id': s['task_id']}, {'$pushAll': {"snl.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})
                tasks.update({'task_id': s['task_id']}, {'$pushAll': {"snl_final.about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            # FIX SUBMISSIONS
            for s in submissions.find({'about._materialsproject.deprecated.crystal_id_deprecated': c_id}, {'submission_id': 1}):
                submissions.update({'submission_id': s['submission_id']}, {'$pushAll': {"about.remarks": ['DEPRECATED', 'SEVERE BUG IN ICSD CONVERSION']}})

            print 'FIXED', c_id


def find_alternate_canonical():
    # see if we can replace a deprecated canonical SNL with a non-deprecated one

    module_dir = os.path.dirname(os.path.abspath(__file__))

    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)
    snl = snldb.snl
    snlgroups = snldb.snlgroups

    for g in snlgroups.find({"canonical_snl.about.remarks":"DEPRECATED"}, {"snlgroup_id": 1, "all_snl_ids": 1}):
        for s in snl.find({"snl_id": {"$in": g['all_snl_ids']}, "about.remarks": {"$ne": "DEPRECATED"}}):
            canonical_mpsnl = MPStructureNL.from_dict(s)
            snldb.switch_canonical_snl(g['snlgroup_id'], canonical_mpsnl)
            print g['snlgroup_id']
            break

    print 'DONE'

def archive_deprecated_fws():
    # find all snlgroups that are deprecated, and archive all WFs that have deprecated fw_ids so we don't run them
    module_dir = os.path.dirname(os.path.abspath(__file__))
    snl_f = os.path.join(module_dir, 'snl.yaml')
    snldb = SNLMongoAdapter.from_file(snl_f)
    snlgroups = snldb.snlgroups

    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)

    for g in snlgroups.find({'canonical_snl.about.remarks':'DEPRECATED'}, {'snlgroup_id': 1}):
        while lpdb.fireworks.find_one({'spec.snlgroup_id': g['snlgroup_id'], 'state': {'$ne': 'ARCHIVED'}}, {'fw_id': 1}):
            fw = lpdb.fireworks.find_one({'spec.snlgroup_id': g['snlgroup_id'], 'state': {'$ne': 'ARCHIVED'}}, {'fw_id': 1})
            print fw['fw_id']
            lpdb.archive_wf(fw['fw_id'])


    print 'DONE'



if __name__ == '__main__':
    archive_deprecated_fws()