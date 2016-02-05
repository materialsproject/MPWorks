import os, sys
from glob import glob
from dateutil import parser
from datetime import datetime
from fireworks.core.launchpad import LaunchPad
from collections import Counter
from fnmatch import fnmatch

lpdb = LaunchPad.from_file('/global/homes/m/matcomp/mp_prod/config/config_Mendel/my_launchpad.yaml')

"""
counter = Counter()
for wf_idx, wf_doc in enumerate(lpdb.workflows.find(
    {'updated_on': {'$exists': 1}},
    {'state': 1, 'updated_on': 1, 'nodes': 1}
)):
     try:
         dt = parser.parse(wf_doc['updated_on'])
     except:
         dt = wf_doc['updated_on']
     counter['ALL_WFS'] += 1
     if dt > datetime(2016, 1, 1):
         counter['ALL_RECENT_WFS'] += 1
         fws_fizzled = []
         for fw_idx, fw_doc in enumerate(lpdb.fireworks.find(
             {'fw_id': {'$in': wf_doc['nodes']}, 'updated_on': {'$exists': 1}, 'spec.task_type': {'$ne': 'GGA Boltztrap'}},
             {'fw_id': 1, 'state': 1, 'updated_on': 1, 'launches': 1, 'spec.task_type': 1}
         )):
             try:
                 dt = parser.parse(fw_doc['updated_on'])
             except:
                 dt = fw_doc['updated_on']
             if dt > datetime(2016, 1, 1):
                 counter['ALL_RECENT_FWS'] += 1
                 counter['FW_' + fw_doc['state']] += 1
                 if fw_doc['state'] == 'FIZZLED':
                     fws_fizzled.append('_'.join([str(fw_doc['fw_id']), fw_doc['spec']['task_type']]))
                     if fnmatch(fw_doc['spec']['task_type'], '*GGA optimize structure*'):
                         lpdb.rerun_fw(fw_doc['fw_id'])
                         print 'rerunning', fw_doc['fw_id'] 
         if fws_fizzled:
             print '{}:{}> {}'.format(counter['ALL_RECENT_WFS'], wf_idx, fws_fizzled)
             if len(fws_fizzled) < 2:
                 sys.exit(0)
print counter
"""

counter = Counter()
for fw_doc in lpdb.fireworks.find(
    {'updated_on': {'$exists': 1}, 'spec.task_type': 'GGA Boltztrap'},
    {'fw_id': 1, 'state': 1, 'updated_on': 1, 'launches': 1}
):
    try:
        dt = parser.parse(fw_doc['updated_on'])
    except:
        dt = fw_doc['updated_on']
    if dt > datetime(2016, 1, 1):
        counter['RECENT_BTZ_FWS_ALL'] += 1
        if fw_doc['state'] == 'RUNNING':
            launch_dir = lpdb.launches.find_one({'launch_id': fw_doc['launches'][0]}, {'launch_dir':1, '_id':0})['launch_dir']
            with open(glob(os.path.join(launch_dir, '*.error'))[0]) as ferr:
                last_line = ferr.readlines()[-1].strip()
                if 'TIME LIMIT' in last_line:
                    lpdb.rerun_fw(fw_doc['fw_id'])
                    print '[{}] rerun due to TIME LIMIT'.format(fw_doc['fw_id'])
                else:
                    counter['RECENT_BTZ_FWS_' + fw_doc['state']] += 1
        else:
             #wf = lpdb.workflows.find_one({'nodes': fw_doc['fw_id']}, {'parent_links':1})
             #parent_fw_id = wf['parent_links'][str(fw_doc['fw_id'])][-1]
             #parent_fw = lpdb.fireworks.find_one({'fw_id': parent_fw_id}, {'state':1})
             #if parent_fw['state'] == 'COMPLETED':
             counter['RECENT_BTZ_FWS_' + fw_doc['state']] += 1
print counter

nfws = 0
for fw_doc in lpdb.fireworks.find(
    {'spec.task_type': 'GGA Boltztrap', 'state': 'FIZZLED'},
    {'fw_id': 1, 'launches': 1, 'state': 1 }
):
    wf = lpdb.workflows.find_one({'nodes': fw_doc['fw_id']}, {'parent_links':1})
    launch_dir = lpdb.launches.find_one({'launch_id': fw_doc['launches'][0]}, {'launch_dir':1, '_id':0})['launch_dir']
    with open(glob(os.path.join(launch_dir, '*.error'))[0]) as ferr:
        last_line = ferr.readlines()[-1].strip()
    if 'parent job unsuccessful' in last_line or 'Could not find task' in last_line:
        parent_fw_id = wf['parent_links'][str(fw_doc['fw_id'])][-1]
        lpdb.rerun_fw(parent_fw_id)
        print '[{}] {} --> marked parent {} for rerun'.format(nfws, fw_doc['fw_id'], parent_fw_id)
    else:
        #lpdb.rerun_fw(fw_doc['fw_id'])
        print '[{}] {} --> {}'.format(nfws, fw_doc['fw_id'], last_line)
    nfws += 1
