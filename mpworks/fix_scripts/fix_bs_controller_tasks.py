from fireworks.core.launchpad import LaunchPad
from fireworks.core.firework import Firework, Workflow
from mpworks.firetasks.controller_tasks import AddEStructureTask
from fireworks.utilities.fw_utilities import get_slug

# DONE manually: "mp-987" -> fw_id: 119629

if __name__ == "__main__":
    lpdb = LaunchPad.from_file('/global/homes/m/matcomp/mp_prod/config/config_Mendel/my_launchpad.yaml')
    spec = {'task_type': 'Controller: add Electronic Structure v2', '_priority': 100000}
    nfws = 0
    for doc in lpdb.fireworks.find(
        {'spec.task_type': 'Controller: add Electronic Structure', 'state': 'COMPLETED', 'spec.analysis': {'$exists':1}},
	{'fw_id': 1, 'spec.analysis.bandgap': 1}
    ):
        fw_id, bandgap = doc['fw_id'], doc['spec']['analysis']['bandgap'] 
        if bandgap > 0 and bandgap < 0.5:
            old_wf = lpdb.workflows.find_one({'nodes':fw_id}, {'parent_links':1,'name':1})
            parent_fw_id = old_wf['parent_links'][str(fw_id)][0]
            f = lpdb.get_wf_summary_dict(fw_id)['name'].replace(' ', '_')
            name = get_slug(f + '--' + spec['task_type'])
            fw = Firework([AddEStructureTask()], spec, name=name)
            print fw_id, name, parent_fw_id
            try:
                lpdb.append_wf(Workflow([fw]), [parent_fw_id])
            except ValueError:
                print 'could not append controller task to wf', old_wf['name']
                continue
	    nfws += 1
	    #break
    print 'nfws =', nfws

# TODO set priorities for child FWs after Controller task has completed
