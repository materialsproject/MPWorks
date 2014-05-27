__author__ = 'weichen'

from fireworks.core.firework import FireWork, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, \
    VaspToDBTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
from pymatgen import Composition
from mpworks.workflows import snl_to_wf
from pymatgen.io.vaspio_set import MPVaspInputSet


def snl_to_wf_customize(snl, parameters=None):
    """
    Run vasp calculations with specified input parameters
    parameters must have:
    "vasp": include vasp run parameters ["poscar", "incar", "kpoints"]
    "task_type": 'optimize structure (2x)', "static" or other task types
    optional parameters:
    "run_tags"(list): unique identifier for duplicate finder
    """

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
        spec['force_mpsnl'] = snl.to_dict
        spec['force_snlgroup_id'] = parameters['snlgroup_id']
        del spec['snl']
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))
    connections[0] = [1]

    # run vasp calculations
    spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['task_type'] = parameters["task_type"]
    spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
    for i in ['incar', 'poscar', 'kpoints']:
        spec['vasp'][i] = snl.data['_vasp'][i].to_dict
    spec['vasp']['potcar'] = MPVaspInputSet().get_potcar(snl.data['_vasp']['poscar'].structure).to_dict
    # Add run tags of pseudopotential
    spec['run_tags'] = spec.get('run_tags', [spec['vasp']['potcar']['functional']])
    #pec['run_tags'].extend(spec['vasp']['potcar']['symbols'])
    spec["run_tags"].extend(parameters["run_tags"])
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 12, 2014'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition.from_formula(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)