__author__ = 'weichen'
from fireworks.core.firework import FireWork, Workflow
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
from mpworks.firetasks.phonon_tasks import update_spec_force_convergence
from pymatgen.io.vaspio import VaspInput, Poscar
import re
from pymatgen.matproj.snl import StructureNL

def get_surface_input(dir):
    '''
    Read a directory containing vasp inputs and converted to snl
    Include surface info in snl.parameters
    '''
    spec = {}

    incar_enforce = {'NPAR': 2, "ISPIN":1}

    vasp_input = VaspInput.from_directory(dir)

    vasp = {}
    print vasp_input.keys()
    vasp['incar'] = vasp_input['INCAR'].to_dict
    vasp['incar'].update(incar_enforce)
    vasp['poscar'] = vasp_input['POSCAR'].to_dict
    vasp['kpoints'] = vasp_input['KPOINTS'].to_dict
    vasp['potcar'] = vasp_input['POTCAR'].to_dict
    if "BULK" in dir:
        vasp["slab"] = False
    else:
        vasp["slab"] = True
    index=re.search(".*(\[.*\]).*", dir)
    if index:
        vasp['index'] = index.group(1)

    snl = StructureNL(vasp_input['POSCAR'].structure, ["Wei Chen <weichen@lbl.gov>"],
                      remarks="Surface", data={"_vasp":vasp})

    return snl

def snl_to_wf_surface(snl, parameters=None):
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

    # run GGA structure optimization for surfaces/bulk
    spec={}
    for i in ['incar', 'poscar', 'potcar', 'kpoints']:
        spec['vasp'][i] = snl.data['_vasp'][i.upper()]
    # Add run tags of pseudopotential
    spec['run_tags'] = spec.get('run_tags', [spec['vasp']['potcar']['functional']])
    spec['run_tags'].extend(spec['vasp']['potcar']['symbols'])

    # Add run tags of +U
    u_tags = ['%s=%s' % t for t in
              zip(Poscar().from_dict(spec['vasp']['poscar']).site_symbols, spec['vasp']['incar'].get('LDAUU', [0] * len(poscar.site_symbols)))]
    spec['run_tags'].extend(u_tags)

    spec['vaspinputset_name'] = "Surfaces"

    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['task_type'] = "Vasp surface"
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition.from_formula(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)