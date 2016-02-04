import os
from glob import glob
from dateutil import parser
from datetime import datetime
from fireworks.core.launchpad import LaunchPad
from collections import Counter
lpdb = LaunchPad.from_file('/global/homes/m/matcomp/mp_prod/config/config_Mendel/my_launchpad.yaml')
counter = Counter()
nfws = 0

for doc in lpdb.fireworks.find(
    {'spec.task_type': 'GGA Boltztrap'},
    {'fw_id': 1, 'state': 1, 'updated_on': 1}
):
    try:
        dt = parser.parse(doc['updated_on'])
    except:
        dt = doc['updated_on']
    if bool(datetime(2016, 1, 25) < dt):
        counter[doc['state']] += 1

for doc in lpdb.fireworks.find(
    {'spec.task_type': 'GGA Boltztrap', 'state': 'FIZZLED'},
    {'fw_id': 1, 'launches': 1, 'state': 1 }
):
    wf = lpdb.workflows.find_one({'nodes': doc['fw_id']}, {'parent_links':1})
    launch_dir = lpdb.launches.find_one({'launch_id': doc['launches'][0]}, {'launch_dir':1, '_id':0})['launch_dir']
    with open(glob(os.path.join(launch_dir, '*.error'))[0]) as ferr:
        last_line = ferr.readlines()[-1].strip()
    if 'parent job unsuccessful' in last_line:
        parent_fw_id = wf['parent_links'][str(doc['fw_id'])][-1]
        lpdb.rerun_fw(parent_fw_id)
        print '[{}] {} --> marked parent {} for rerun'.format(nfws, doc['fw_id'], parent_fw_id)
    else:
        #lpdb.rerun_fw(doc['fw_id'])
        print '[{}] {} --> {}'.format(nfws, doc['fw_id'], last_line)
    counter[last_line] += 1
    nfws += 1
print counter
