import os, json
from pymongo import DESCENDING, ASCENDING
from fireworks.fw_config import CONFIG_FILE_DIR, SORT_FWS
from fireworks.core.fworker import FWorker
from fireworks.core.launchpad import LaunchPad
from pymongo import ReturnDocument

launchpad = LaunchPad.from_file(os.path.join(CONFIG_FILE_DIR, 'my_launchpad.yaml'))
fworker = FWorker.from_file(os.path.join(CONFIG_FILE_DIR, 'my_fworker.yaml'))
#print launchpad._get_a_fw_to_run(query=fworker.query, checkout=False)
m_query = dict(fworker.query)
m_query['state'] = 'READY'
sortby = [("spec._priority", DESCENDING)]
if SORT_FWS.upper() == "FIFO":
    sortby.append(("created_on", ASCENDING))
elif SORT_FWS.upper() == "FILO":
    sortby.append(("created_on", DESCENDING))
#print json.dumps(m_query, indent=4)
projection = {
    '_id': 0, 'fw_id': 1, 'spec._fworker': 1, 'spec.task_type': 1, 'spec._queueadapter': 1,
    'spec.mpsnl.about.remarks': 1, 'spec.snl.about.remarks': 1, 'spec.prev_vasp_dir': 1,
    'updated_on': 1, 'state': 1
}

fw_ids = []
for idoc, doc in enumerate(launchpad.fireworks.find(m_query, projection=projection, sort=sortby).limit(100)):
    #print doc
    if 'walltime' in doc['spec']['_queueadapter']:
        walltime = doc['spec']['_queueadapter']['walltime'] 
        if int(walltime.split(':')[0]) > 48:
            launchpad.fireworks.find_one_and_update(
                {'fw_id': doc['fw_id']}, 
                {'$set': {'spec._queueadapter.walltime': '48:00:00'}},
                projection=projection,
                return_document=ReturnDocument.AFTER
            )
            print doc['fw_id'], '----> walltime updated'
    if 'nnodes' in doc['spec']['_queueadapter'] and not 'nodes' in doc['spec']['_queueadapter']:
        launchpad.fireworks.find_one_and_update(
            {'fw_id': doc['fw_id']}, 
            {'$rename': {'spec._queueadapter.nnodes': 'spec._queueadapter.nodes'}},
            projection=projection,
            return_document=ReturnDocument.AFTER
        )
        print doc['fw_id'], '----> nodes key renamed' 
    if 'pre_rocket' in doc['spec']['_queueadapter']:
        launchpad.fireworks.find_one_and_update(
            m_query,
            {'$unset' : { 'spec._queueadapter.pre_rocket' : 1}},
            projection=projection,
            return_document=ReturnDocument.AFTER
        )
        print doc['fw_id'], '----> pre_rocket dropped' 
    if 'prev_vasp_dir' in doc['spec'] and not os.path.exists(doc['spec']['prev_vasp_dir']):
        block_dir = doc['spec']['prev_vasp_dir'].split('/')[-2:]
        launch_dir = '/'.join('/oasis/projects/nsf/csd436/phuck/garden'.split('/') + block_dir)
        if not os.path.exists(launch_dir):
            print doc['fw_id'], '---->', '/'.join(block_dir), 'does not exists!'
	    continue
    fw_ids.append(doc['fw_id'])
print 'fixed', fw_ids
