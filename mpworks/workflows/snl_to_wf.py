from collections import defaultdict
from fireworks.core.firework import Firework, Workflow, Tracker
from fireworks.utilities.fw_utilities import get_slug
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp, DupeFinderDB
from mpworks.firetasks.controller_tasks import AddEStructureTask
from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, \
    VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
from pymatgen import Composition
from pymatgen.io.cif import CifParser
from pymatgen.io.vasp.sets import MPVaspInputSet, MPGGAVaspInputSet
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


def _snl_to_spec(snl, enforce_gga=False, parameters=None):

    parameters = parameters if parameters else {}
    parameters['boltztrap'] = parameters.get('boltztrap', True)  # by default run boltztrap
    spec = {'parameters': parameters}

    incar_enforce = {'NPAR': 2}
    if 'exact_structure' in parameters and parameters['exact_structure']:
        structure = snl.structure
    else:
        structure = snl.structure.get_primitive_structure()

    mpvis = MPGGAVaspInputSet(user_incar_settings=incar_enforce) if enforce_gga else MPVaspInputSet(user_incar_settings=incar_enforce)

    incar = mpvis.get_incar(structure)
    poscar = mpvis.get_poscar(structure)
    kpoints = mpvis.get_kpoints(structure)
    potcar = mpvis.get_potcar(structure)

    spec['vasp'] = {}
    spec['vasp']['incar'] = incar.as_dict()
    spec['vasp']['poscar'] = poscar.as_dict()
    spec['vasp']['kpoints'] = kpoints.as_dict()
    spec['vasp']['potcar'] = potcar.as_dict()

    # Add run tags of pseudopotential
    spec['run_tags'] = spec.get('run_tags', [potcar.functional])
    spec['run_tags'].extend(potcar.symbols)

    # Add run tags of +U
    u_tags = ['%s=%s' % t for t in
              zip(poscar.site_symbols, incar.get('LDAUU', [0] * len(poscar.site_symbols)))]
    spec['run_tags'].extend(u_tags)

    # add user run tags
    if 'run_tags' in parameters:
        spec['run_tags'].extend(parameters['run_tags'])
        del spec['parameters']['run_tags']

    # add exact structure run tag automatically if we have a unique situation
    if 'exact_structure' in parameters and parameters['exact_structure'] and snl.structure != snl.structure.get_primitive_structure():
        spec['run_tags'].extend('exact_structure')

    spec['_dupefinder'] = DupeFinderVasp().to_dict()
    spec['vaspinputset_name'] = mpvis.__class__.__name__
    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp'][
        'incar'].get('LDAU', False) else 'GGA optimize structure (2x)'

    return spec

def snl_to_wf(snl, parameters=None):
    fws = []
    connections = defaultdict(list)
    parameters = parameters if parameters else {}

    snl_priority = parameters.get('priority', 1)
    priority = snl_priority * 2  # once we start a job, keep going!

    f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

    snl_spec = {}
    if 'snlgroup_id' in parameters:
        if 'mpsnl' in parameters:
            snl_spec['mpsnl'] = parameters['mpsnl']
        elif isinstance(snl, MPStructureNL):
            snl_spec['mpsnl'] = snl.as_dict()
        else:
            raise ValueError("improper use of force SNL")
        snl_spec['snlgroup_id'] = parameters['snlgroup_id']
    else:
        # add the SNL to the SNL DB and figure out duplicate group
        tasks = [AddSNLTask()]
        spec = {'task_type': 'Add to SNL database', 'snl': snl.as_dict(), '_queueadapter': QA_DB, '_priority': snl_priority}
        fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))
        connections[0] = [1]

    trackers = [Tracker('FW_job.out'), Tracker('FW_job.error'), Tracker('vasp.out'), Tracker('OUTCAR'), Tracker('OSZICAR'), Tracker('OUTCAR.relax1'), Tracker('OUTCAR.relax2')]
    trackers_db = [Tracker('FW_job.out'), Tracker('FW_job.error')]
    # run GGA structure optimization
    spec = _snl_to_spec(snl, enforce_gga=True, parameters=parameters)
    spec.update(snl_spec)
    spec['_priority'] = priority
    spec['_queueadapter'] = QA_VASP
    spec['_trackers'] = trackers
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': priority*2,
            '_allow_fizzled_parents': True, '_queueadapter': QA_DB, "_dupefinder": DupeFinderDB().to_dict(), '_trackers': trackers_db}
    fws.append(
        Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    # determine if GGA+U FW is needed
    incar = MPVaspInputSet().get_incar(snl.structure).as_dict()
    ggau_compound = ('LDAU' in incar and incar['LDAU'])

    if not parameters.get('skip_bandstructure', False) and (not ggau_compound or parameters.get('force_gga_bandstructure', False)):
        spec = {'task_type': 'Controller: add Electronic Structure v2', '_priority': priority,
                '_queueadapter': QA_CONTROL}
        fws.append(
            Firework([AddEStructureTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                     fw_id=3))
        connections[2] = [3]

    if ggau_compound:
        spec = _snl_to_spec(snl, enforce_gga=False, parameters=parameters)
        del spec['vasp']  # we are stealing all VASP params and such from previous run
        spec['_priority'] = priority
        spec['_queueadapter'] = QA_VASP
        spec['_trackers'] = trackers
        fws.append(Firework(
            [VaspCopyTask(), SetupGGAUTask(),
             get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']),
            fw_id=10))
        connections[2].append(10)

        spec = {'task_type': 'VASP db insertion', '_queueadapter': QA_DB,
                '_allow_fizzled_parents': True, '_priority': priority, "_dupefinder": DupeFinderDB().to_dict(), '_trackers': trackers_db}
        fws.append(
            Firework([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=11))
        connections[10] = [11]

        if not parameters.get('skip_bandstructure', False):
            spec = {'task_type': 'Controller: add Electronic Structure v2', '_priority': priority,
                    '_queueadapter': QA_CONTROL}
            fws.append(
                Firework([AddEStructureTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                         fw_id=12))
            connections[11] = [12]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'  # not maintained

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']
    return Workflow(fws, connections, name=Composition(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)


"""
def snl_to_wf_ggau(snl):

    # TODO: add WF meta

    fws = []
    connections = {}

    # add the root FW (GGA+U)
    spec = _snl_to_spec(snl, enforce_gga=False)
    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(Firework(tasks, spec, fw_id=1))

    # add GGA insertion to DB
    spec = {'task_type': 'VASP db insertion', '_priority': 2,
            '_category': 'VASP', '_queueadapter': QA_VASP}
    fws.append(Firework([VaspToDBTask()], spec, fw_id=2))
    connections[1] = 2
    mpvis = MPVaspInputSet()

    spec['vaspinputset_name'] = mpvis.__class__.__name__

    return Workflow(fws, connections, name=Composition(snl.structure.composition.reduced_formula).alphabetical_formula)
"""


if __name__ == '__main__':
    s1 = CifParser('test_wfs/Si.cif').get_structures()[0]
    s2 = CifParser('test_wfs/FeO.cif').get_structures()[0]

    snl1 = StructureNL(s1, "Anubhav Jain <ajain@lbl.gov>")
    snl2 = StructureNL(s2, "Anubhav Jain <ajain@lbl.gov>")

    snl_to_wf(snl1).to_file('test_wfs/wf_si_dupes.json', indent=4)
    snl_to_wf(snl2).to_file('test_wfs/wf_feo_dupes.json', indent=4)
