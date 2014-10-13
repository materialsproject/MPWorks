from fireworks.core.firework import FireWork
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, VaspToDBTask

__author__ = 'Ioannis Petousis'

from fireworks.utilities.fw_utilities import get_slug
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.workflows.wf_settings import QA_DB, QA_VASP
from pymatgen import Composition
from pymatgen.io.vaspio_set import MPStaticDielectricDFPTVaspInputSet
from pymatgen.io.vaspio.vasp_input import Incar, Poscar, Kpoints
from fireworks.core.firework import FireWork, Workflow

from mpworks.workflows import snl_to_wf

# from mpworks.firetasks.dielectrics_tasks import update_spec_static_dielectrics_convergence



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
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))
    
    # run GGA structure optimization for static dielectric convergence
    spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
    mpvis = MPStaticDielectricDFPTVaspInputSet()
    incar = mpvis.get_incar(snl.structure)
    incar.update({"EDIFF":"1.0E-6"})
    incar.update({"ENCUT":"800"})
    spec['vasp']['incar'] = incar.to_dict
    kpoints_density = 3000
    k=Kpoints.automatic_density(snl.structure, kpoints_density)
    spec['vasp']['kpoints'] = k.to_dict
    # spec = update_spec_static_dielectrics_convergence(spec)
    # del spec['dupefinder']
    # spec['run_tags'].append("origin")
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['task_type'] = "Static Dielectrics Calculation" # Change name here: delete Vasp? 
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))
    connections[0] = [1] # define fw_id=1 is dependent on completion of fw_id=0

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority, '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
    fws.append(FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2] # define fw_id=2 is dependent on completion of fw_id=1

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition.from_formula(snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)