
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.workflows.wf_settings import QA_DB
from pymatgen import Composition










def snl_to_wf_static_dielectrics(snl, parameters=None):
    fws = []
    connections = {}
    parameters = parameters if parameters else {}

    snl_priority = parameters.get('priority', 1)
    priority = snl_priority * 2  # once we start a job, keep going!

    f = Composition.from_formula(snl.structure.composition.reduced_formula).alphabetical_formula
    
    # add the SNL to the SNL DB and figure out duplicate group
    tasks = [AddSNLTask()]
	spec = {'task_type': 'Add to SNL database', 'snl': snl.to_dict, '_queueadapter': QA_DB, '_priority': snl_priority}
    if 'snlgroup_id' in parameters and isinstance(snl, MPStructureNL):
        spec['static_dielectrics_mpsnl'] = snl.to_dict
        spec['static_dielectrics_snlgroup_id'] = parameters['snlgroup_id']
        del spec['snl']
