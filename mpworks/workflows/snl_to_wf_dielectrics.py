from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, VaspToDBTask
from mpworks.firetasks.dielectrics_tasks import SetupDFPTDielectricsTask

__author__ = 'Ioannis Petousis'

from fireworks.utilities.fw_utilities import get_slug
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspToDBTask
from mpworks.firetasks.raman_tasks import SetupRamanTask
from mpworks.firetasks.dielectrics_tasks import SetupDFPTDielectricsTask
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
from pymatgen import Composition
from pymatgen.core.structure import Structure
from pymatgen.io.vasp.sets import MPStaticDielectricDFPTVaspInputSet, MPVaspInputSet
from pymatgen.io.vasp.inputs import Incar, Poscar, Kpoints
from fireworks.core.firework import Firework, Workflow
from mpworks.workflows import snl_to_wf


def high_forces_in_struct(material_id, force_limit=0.05):
    """
    Function that checks if the forces in the relaxed structure on the
    Materials Project website are less than a defined force_limit.
    Returns True if Yes, False if No.
    """
    from pymongo import MongoClient

    uri = "mongodb://read_only_paul:pHK4XtwGbW7CXH@mongodb04.nersc.gov/jcesr_prod"
    client = MongoClient(uri)
    vaspDB = client.jcesr_prod

    materials = vaspDB.materials.find({"task_id":material_id})

    if materials.count() != 1:
        return True

    task_ids = materials[0]['task_ids']

    task_types = []
    for task_id in task_ids:
	materials = vaspDB.tasks.find({"task_id":task_id})
	task_types.append(materials[0]['task_type'])

    if 'GGA+U optimize structure (2x)' in task_types:
        task_id = task_ids[task_types.index('GGA+U optimize structure (2x)')]
    else:
        if 'GGA optimize structure (2x)' in task_types:
            task_id = task_ids[task_types.index('GGA optimize structure (2x)')]
        else:
            print 'Problem no relaxation calculation found'

    compounds = vaspDB.tasks.find({"task_id":task_id})
    if compounds.count() != 1:
        return True
    force_tensor = compounds[0]['calculations'][-1]['output']["ionic_steps"][-1]["forces"]
    high_force = False
    for atom in force_tensor:
    	for direction in atom:
            if abs(direction) > force_limit:
                high_force = True
                force_mag = direction

    if high_force is True:
        return True

    return False


def snl_to_wf_static_dielectrics(snl, parameters=None):
    fws = []
    connections = {}
    parameters = parameters if parameters else {}

    snl_priority = parameters.get('priority', 1)
    priority = snl_priority * 2  # once we start a job, keep going!

    f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

    # add the SNL to the SNL DB and figure out duplicate group
    tasks = [AddSNLTask()]
    spec = {'task_type': 'Add to SNL database', 'snl': snl.as_dict(), '_queueadapter': QA_DB, '_priority': snl_priority}
    if 'snlgroup_id' in parameters and isinstance(snl, MPStructureNL):
        spec['static_dielectrics_mpsnl'] = snl.as_dict()
        spec['static_dielectrics_snlgroup_id'] = parameters['snlgroup_id']
        del spec['snl']
    fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))


    if high_forces_in_struct(parameters['mp_id'], force_limit=0.05) is True:
        # run optmization with force as convergence criterion:
        spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
        mpvis = MPVaspInputSet()
        incar = mpvis.get_incar(snl.structure)

        incar.update({"EDIFFG":'-0.05', "NPAR":"2"})
        spec['vasp']['incar'] = incar.as_dict()
        del spec['_dupefinder']
        # spec['run_tags'].append("origin")
        spec['_priority'] = priority
        spec["_pass_job_info"] = True
        spec['_allow_fizzled_parents'] = False
        spec['_queueadapter'] = QA_VASP
        spec['task_type'] = "force convergence" # Change name here: delete Vasp?
        tasks = [VaspWriterTask(), get_custodian_task(spec)]
        fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))
        connections[0] = [1] # define fw_id=1 is dependent on completion of fw_id=0

        # insert into DB - Force optimization
        spec = {'task_type': 'VASP db insertion', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
        fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
        connections[1] = [2] # define fw_id=2 is dependent on completion of fw_id=1

        spec= {'task_type': 'Setup DFPT Dielectrics Task', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': False, '_queueadapter': QA_CONTROL}
        fws.append(Firework([SetupDFPTDielectricsTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=3))
        connections[2] = [3]

        wf_meta = get_meta_from_structure(snl.structure)
        wf_meta['run_version'] = 'May 2013 (1)'

        if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
            wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

        return Workflow(fws, connections, name=Composition(snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)



    # run DFPT for static dielectrics run:
    # if 'force_convergence' in snl.projects:
    #     relaxed_structure = spec['output']['crystal']
    #     spec['vasp']['poscar'] = relaxed_structure
    #     poscar = mpvis.get_poscar(Structure.from_dict(spec['output']['crystal']))

    spec = snl_to_wf._snl_to_spec(snl, parameters=parameters)
    mpvis = MPStaticDielectricDFPTVaspInputSet()
    incar = mpvis.get_incar(snl.structure)
    incar.update({"EDIFF":"1.0E-6", "ENCUT":"600", "NPAR":"2", "NWRITE":"3"})
    # incar.update({"ALGO":"Normal"})
    spec['vasp']['incar'] = incar.as_dict()
    kpoints_density = 3000
    k=Kpoints.automatic_density(snl.structure, kpoints_density, force_gamma=True)
    spec['vasp']['kpoints'] = k.as_dict()
    del spec['_dupefinder']
    # spec['run_tags'].append("origin")
    spec['_priority'] = priority
    spec["_pass_job_info"] = True
    spec['_allow_fizzled_parents'] = False
    spec['_queueadapter'] = QA_VASP
    spec['task_type'] = "Static Dielectrics"
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))
    connections[0] = [1] # define fw_id=1 is dependent on completion of fw_id=0

    # insert into DB - Static Dielectrics run
    spec = {'task_type': 'VASP db insertion', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
    fws.append(Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2,3] # define fw_id=2 is dependent on completion of fw_id=1

    # Setup Raman Calculation:
    spec= {'task_type': 'Setup Raman Task', '_priority': priority, "_pass_job_info": True, '_allow_fizzled_parents': False, '_queueadapter': QA_CONTROL}
    spec['passed_vars'] = []
    fws.append(Firework([SetupRamanTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=3))


    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition(snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)
