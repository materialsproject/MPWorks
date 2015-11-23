import time
from fireworks.core.launchpad import LaunchPad
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.controller_tasks import AddEStructureTask
from fireworks.utilities.fw_utilities import get_slug

# DONE manually: "mp-987" -> fw_id: 119629

lpdb = LaunchPad.from_file('/global/homes/m/matcomp/mp_prod/config/config_Mendel/my_launchpad.yaml')
spec = {'task_type': 'Controller: add Electronic Structure v2', '_priority': 100000}

def append_wf(fw_id, parent_fw_id=None):
    wf = lpdb.workflows.find_one({'nodes':fw_id}, {'parent_links':1,'links':1,'name':1})
    try:
        if parent_fw_id is None:
            parent_fw_id = wf['parent_links'][str(fw_id)][-1]
        # non-defused AddEStructureTask v2 already in children?
        for child_fw_id in wf['links'][str(parent_fw_id)]:
            if child_fw_id == parent_fw_id: continue
            child_fw = lpdb.fireworks.find_one({'fw_id': child_fw_id}, {'spec.task_type':1, 'state':1})
            if child_fw['spec']['task_type'] == 'Controller: add Electronic Structure v2':
                if child_fw['state'] == 'DEFUSED':
                    lpdb.reignite_fw(child_fw_id)
                    print 'AddEStructureTask v2', child_fw_id , 'reignited for', fw_id
                elif child_fw['state'] == 'FIZZLED':
                    lpdb.rerun_fw(child_fw_id)
                    print 'AddEStructureTask v2', child_fw_id , 'marked for rerun for', fw_id
                elif child_fw['state'] == 'COMPLETED':
                    print 'AddEStructureTask v2 already successfully run for', fw_id
                    sec_child_fw_id = wf['links'][str(child_fw_id)][0]
		    sec_child_fw = lpdb.fireworks.find_one({'fw_id': sec_child_fw_id}, {'spec.task_type':1, 'state':1})
		    if sec_child_fw['state'] == 'FIZZLED':
                        lpdb.rerun_fw(sec_child_fw_id)
		        print 'FIZZLED -> marked for rerun:', sec_child_fw_id, sec_child_fw['spec']['task_type']
                else:
                    print 'AddEStructureTask v2 added but neither DEFUSED, FIZZLED, or COMPLETED for', fw_id
                return
        f = lpdb.get_wf_summary_dict(fw_id)['name'].replace(' ', '_')
        name = get_slug(f + '--' + spec['task_type'])
        fw = Firework([AddEStructureTask()], spec, name=name)
        lpdb.append_wf(Workflow([fw]), [parent_fw_id])
        print name, 'added for', fw_id
    except ValueError:
        raise ValueError('could not append controller task to wf', wf['name'])

if __name__ == "__main__":
    nfws = 0
    #append_wf(42391, parent_fw_id=69272)
    #append_wf(51449, parent_fw_id=76078)
    #for doc in lpdb.fireworks.find(
    #    {'spec.task_type': 'Controller: add Electronic Structure v2', 'spec._priority': {'$exists':1}},
    #    {'fw_id': 1, 'spec._priority': 1, 'state': 1 }
    #):
    #    if (doc['state'] == 'FIZZLED' or doc['state'] == 'READY') and doc['spec']['_priority'] == 100000:
    #        print nfws, doc['fw_id']
    #        lpdb.defuse_fw(doc['fw_id'])
    #        nfws += 1
    for doc in lpdb.fireworks.find(
        {'spec.task_type': 'Controller: add Electronic Structure', 'state': 'COMPLETED', 'spec.analysis': {'$exists':1}, 'fw_id': {'$gte': 155067}}, # new controllers added -> fizzled GGA static reruns
        {'fw_id': 1, 'spec.analysis.bandgap': 1}
    ):
        fw_id, bandgap = doc['fw_id'], doc['spec']['analysis']['bandgap'] 
        if bandgap > 0 and bandgap < 0.5:
            try:
                append_wf(fw_id)
                time.sleep(.5)
            except ValueError:
                continue
            nfws += 1
            #if nfws > 10: break
    print 'nfws =', nfws

# TODO set priorities for child FWs after Controller task has completed
