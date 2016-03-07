from pymatgen.io.vasp import Poscar
from mpworks.firetasks.elastic_tasks import SetupElastConstTask, SetupFConvergenceTask, SetupDeformedStructTask

__author__ = 'weichen'

from fireworks.core.firework import Firework, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, \
    VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
from pymatgen import Composition
from mpworks.workflows import snl_to_wf
from mpworks.firetasks.elastic_tasks import update_spec_force_convergence
from collections import defaultdict


def snl_to_wf_elastic(snl, parameters):
    # parameters["user_vasp_settings"] specifies user defined incar/kpoints parameters
    fws = []
    connections = defaultdict(list)
    parameters = parameters if parameters else {}

    snl_priority = parameters.get('priority', 1)
    priority = snl_priority * 2  # once we start a job, keep going!

    f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

    # add the SNL to the SNL DB and figure out duplicate group
    tasks = [AddSNLTask()]
    spec = {'task_type': 'Add to SNL database', 'snl': snl.as_dict(), 
            '_queueadapter': QA_DB, '_priority': snl_priority}
    if 'snlgroup_id' in parameters and isinstance(snl, MPStructureNL):
        spec['force_mpsnl'] = snl.as_dict()
        spec['force_snlgroup_id'] = parameters['snlgroup_id']
        del spec['snl']
    fws.append(Firework(tasks, spec, 
                        name=get_slug(f + '--' + spec['task_type']), fw_id=0))
    connections[0] = [1]

    parameters["exact_structure"] = True
    # run GGA structure optimization for force convergence
    spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
    user_vasp_settings = parameters.get("user_vasp_settings")
    spec = update_spec_force_convergence(spec, user_vasp_settings)
    spec['run_tags'].append("origin")
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    del spec['_dupefinder']
    spec['task_type'] = "Vasp force convergence optimize structure (2x)"
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(Firework(tasks, spec, 
                        name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, 
            'clean_task_doc':True, 'elastic_constant':"force_convergence"}
    fws.append(Firework([VaspToDBTask()], spec, 
                        name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    spec = {'task_type': 'Setup Deformed Struct Task', '_priority': priority,
                '_queueadapter': QA_CONTROL}
    fws.append(Firework([SetupDeformedStructTask()], spec, 
                        name=get_slug(f + '--' + spec['task_type']),fw_id=3))
    connections[2] = [3]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)
