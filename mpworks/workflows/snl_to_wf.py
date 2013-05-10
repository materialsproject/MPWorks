from fireworks.core.firework import FireWork, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.controller_tasks import AddEStructureTask
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, \
    VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_utils import _get_custodian_task
from pymatgen import Composition
from pymatgen.io.cifio import CifParser
from pymatgen.io.vaspio_set import MPVaspInputSet, MPGGAVaspInputSet
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


def _snl_to_spec(snl, enforce_gga=False):
    spec = {}

    incar_enforce = {'NPAR': 2}
    structure = snl.structure
    mpvis = MPGGAVaspInputSet(incar_enforce) if enforce_gga else MPVaspInputSet(incar_enforce)

    incar = mpvis.get_incar(structure)
    poscar = mpvis.get_poscar(structure)
    kpoints = mpvis.get_kpoints(structure)
    potcar = mpvis.get_potcar(structure)

    spec['vasp'] = {}
    spec['vasp']['incar'] = incar.to_dict
    spec['vasp']['poscar'] = poscar.to_dict
    spec['vasp']['kpoints'] = kpoints.to_dict
    spec['vasp']['potcar'] = potcar.to_dict

    # Add run tags of pseudopotential
    spec['run_tags'] = spec.get('run_tags', [potcar.functional])
    spec['run_tags'].extend(potcar.symbols)

    # Add run tags of +U
    u_tags = ['%s=%s' % t for t in
              zip(poscar.site_symbols, incar.get('LDAUU', [0] * len(poscar.site_symbols)))]
    spec['run_tags'].extend(u_tags)

    spec['_dupefinder'] = DupeFinderVasp().to_dict()
    spec['vaspinputset_name'] = mpvis.__class__.__name__
    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp'][
        'incar'].get('LDAU', False) else 'GGA optimize structure (2x)'

    return spec


def snl_to_wf(snl, parameters=None):
    fws = []
    connections = {}
    parameters = parameters if parameters else {}

    f = Composition.from_formula(snl.structure.composition.reduced_formula).alphabetical_formula

    # add the SNL to the SNL DB and figure out duplicate group
    tasks = [AddSNLTask()]
    spec = {'task_type': 'Add to SNL database', 'snl': snl.to_dict, '_queueadapter': {'nnodes': 1}}
    if 'snlgroup_id' in parameters and isinstance(snl, MPStructureNL):
        spec['force_mpsnl'] = snl.to_dict
        spec['force_snlgroup_id'] = parameters['snlgroup_id']
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))
    connections[0] = [1]

    # run GGA structure optimization
    spec = _snl_to_spec(snl, enforce_gga=True)
    spec['_priority'] = 2
    tasks = [VaspWriterTask(), _get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=1))

    # insert into DB - GGA structure optimization
    spec = {'task_type': 'VASP db insertion', '_priority': 2,
            '_allow_fizzled_parents': True, '_queueadapter': {'nnodes': 1}}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=2))
    connections[1] = [2]

    if not parameters.get('skip_bandstructure', False):
        spec = {'task_type': 'Controller: add Electronic Structure', '_priority': 2,
                '_queueadapter': {'nnodes': 1}}
        fws.append(
            FireWork([AddEStructureTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                     fw_id=3))
        connections[2] = [3]

    # determine if GGA+U FW is needed
    incar = MPVaspInputSet().get_incar(snl.structure).to_dict

    if 'LDAU' in incar and incar['LDAU']:
        spec = _snl_to_spec(snl, enforce_gga=False)
        del spec['vasp']  # we are stealing all VASP params and such from previous run
        spec['_priority'] = 2
        fws.append(FireWork(
            [VaspCopyTask(), SetupGGAUTask(),
             _get_custodian_task(spec)], spec, name=get_slug(f + '--' + spec['task_type']),
            fw_id=10))
        connections[2].append(10)

        spec = {'task_type': 'VASP db insertion', '_queueadapter': {'nnodes': 1},
                '_allow_fizzled_parents': True, '_priority': 2}
        fws.append(
            FireWork([VaspToDBTask()], spec, name=get_slug(f + '--' + spec['task_type']), fw_id=11))
        connections[10] = [11]

        if not parameters.get('skip_bandstructure', False):
            spec = {'task_type': 'Controller: add Electronic Structure', '_priority': 2,
                    '_queueadapter': {'nnodes': 1}}
            fws.append(
                FireWork([AddEStructureTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                         fw_id=12))
            connections[11] = [12]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']
    return Workflow(fws, connections, name=Composition.from_formula(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)


"""
def snl_to_wf_ggau(snl):

    # TODO: add WF meta

    fws = []
    connections = {}

    # add the root FW (GGA+U)
    spec = _snl_to_spec(snl, enforce_gga=False)
    tasks = [VaspWriterTask(), _get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, fw_id=1))

    # add GGA insertion to DB
    spec = {'task_type': 'VASP db insertion', '_priority': 2,
            '_category': 'VASP', '_queueadapter': {'nnodes': 1}}
    fws.append(FireWork([VaspToDBTask()], spec, fw_id=2))
    connections[1] = 2
    mpvis = MPVaspInputSet()

    spec['vaspinputset_name'] = mpvis.__class__.__name__

    return Workflow(fws, connections, name=Composition.from_formula(snl.structure.composition.reduced_formula).alphabetical_formula)
"""

if __name__ == '__main__':
    s1 = CifParser('test_wfs/Si.cif').get_structures()[0]
    s2 = CifParser('test_wfs/FeO.cif').get_structures()[0]

    snl1 = StructureNL(s1, "Anubhav Jain <ajain@lbl.gov>")
    snl2 = StructureNL(s2, "Anubhav Jain <ajain@lbl.gov>")

    snl_to_wf(snl1).to_file('test_wfs/wf_si_dupes.json', indent=4)
    snl_to_wf(snl2).to_file('test_wfs/wf_feo_dupes.json', indent=4)
