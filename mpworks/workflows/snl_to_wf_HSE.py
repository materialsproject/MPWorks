__author__ = 'Qimin'
__date__ = 'July 5, 2014'

from collections import defaultdict
from fireworks.core.firework import FireWork, Workflow, Tracker
from fireworks.utilities.fw_utilities import get_slug
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp, DupeFinderDB
from mpworks.firetasks.vasp_setup_tasks import import AddHSEBSTask
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspCopyTask, VaspToDBTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
from pymatgen import Composition

def _snl_to_spec_HSE(snl, parameters=None):
    spec = {}
    parameters = parameters if parameters else {}

    incar_enforce = {'NPAR': 2}
    if 'exact_structure' in parameters and parameters['exact_structure']:
        structure = snl.structure
    else:
        structure = snl.structure.get_primitive_structure()

    mphsevis = MPHSEVaspInputSet(user_incar_settings=incar_enforce)

    incar = mphsevis.get_incar(structure)
    poscar = mphsevis.get_poscar(structure)
    kpoints = mphsevis.get_kpoints(structure)
    potcar = mphsevis.get_potcar(structure)

    spec['vasp'] = {}
    spec['vasp']['incar'] = incar.to_dict
    spec['vasp']['poscar'] = poscar.to_dict
    spec['vasp']['kpoints'] = kpoints.to_dict
    spec['vasp']['potcar'] = potcar.to_dict

    # Add run tags of pseudopotential
    spec['run_tags'] = spec.get('run_tags', [potcar.functional])
    spec['run_tags'].extend(potcar.symbols)

    spec['_dupefinder'] = DupeFinderVasp().to_dict()
    spec['vaspinputset_name'] = mpvis.__class__.__name__
    spec['task_type'] = 'HSE optimize structure (2x)'
    return spec

def snl_to_wf_HSE(snl, parameters=None):
    fws = []
    connections = defaultdict(list)
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

    parameters["exact_structure"] = True

    # run HSE structure optimization
    spec = _snl_to_spec_HSE(snl, parameters=parameters)
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['task_type'] = "HSE optimize structure"
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - HSE structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority*2,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, "_dupefinder": DupeFinderDB().to_dict()}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    # copy VASP output
    spec = _snl_to_spec(snl, parameters=parameters)
    del spec['vasp']  # we are stealing all VASP params and such from previous run
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['_trackers'] = trackers
    fws.append(FireWork(
        [VaspCopyTask(), get_custodian_task(spec)],
        spec, name=get_slug(f + '--' + spec['task_type']), fw_id=3))
    connections[2].append(3)

    # run HSE band structure calculation
    if not parameters.get('skip_bandstructure', False):
        spec = {'task_type': 'Controller: add HSE Electronic Structure', '_priority': priority,
                '_queueadapter': QA_CONTROL}
        fws.append(
            FireWork([AddHSEBSTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                     fw_id=4))
        connections[3] = [4]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'July 2014'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition.from_formula(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)