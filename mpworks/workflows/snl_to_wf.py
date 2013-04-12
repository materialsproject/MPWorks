from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, PoscarErrorHandler
from fireworks.core.firework import FireWork, Workflow
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.custodian_task import VaspCustodianTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask, SetupStaticRunTask, SetupNonSCFTask
from pymatgen.io.cifio import CifParser
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet, MaterialsProjectGGAVaspInputSet
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'

# TODO: add duplicate checks for DB task - don't want to add the same dir twice!!
# TODO: different walltime requirements and priority for DB task


def _get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    if 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
    else:
        jobs = [VaspJob(v_exe)]

    handlers = [VaspErrorHandler(), PoscarErrorHandler()]
    params = {'jobs': [j.to_dict for j in jobs], 'handlers': [h.to_dict for h in handlers], 'max_errors': 10}

    return VaspCustodianTask(params)


def _snl_to_spec(snl, enforce_gga=True):
    spec = {}

    mpvis = MaterialsProjectGGAVaspInputSet() if enforce_gga else MaterialsProjectVaspInputSet()
    structure = snl.structure

    spec['vasp'] = {}
    spec['vasp']['incar'] = mpvis.get_incar(structure).to_dict
    spec['vasp']['incar']['NPAR'] = 2
    spec['vasp']['poscar'] = mpvis.get_poscar(structure).to_dict
    spec['vasp']['kpoints'] = mpvis.get_kpoints(structure).to_dict
    spec['vasp']['potcar'] = mpvis.get_potcar(structure).to_dict
    spec['_dupefinder'] = DupeFinderVasp().to_dict()
    spec['_priority'] = 2
    spec['elements'] = [e.symbol for e in snl.structure.composition.elements]
    spec['nelements'] = len(spec['elements'])
    spec['formula_abc'] = snl.structure.composition.alphabetical_formula
    spec['formula_red'] = snl.structure.composition.reduced_formula
    # spec['_category'] = 'VASP'
    spec['vaspinputset_name'] = mpvis.__class__.__name__

    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp']['incar'].get('LDAU', False) else 'GGA optimize structure (2x)'

    spec.update(_get_metadata(snl))

    return spec


def _get_metadata(snl):
    md = {'run_tags': ['auto generation v1.0'], 'snl': snl.to_dict}

    try:
        sd = snl.data['_materialsproject']
        md['snl_id'] = sd['snl_id']
        md['snlgroup_id'] = sd['snlgroup_id']
        md['snlgroupSG_id'] = sd['snlgroupSG_id']
        md['submission_id'] = sd.get('submission_id', None)
    except:
        raise ValueError("SNL must contain snl_id, snlgroup_id, snlgroupSG_id in data['_materialsproject']['snl_id']")

    return md


def snl_to_wf(snl):
    # TODO: clean this up once we're out of testing mode
    # TODO: add WF metadata
    fws = []
    connections = {}
    # add the root FW (GGA)
    spec = _snl_to_spec(snl, enforce_gga=True)
    tasks = [VaspWriterTask(), _get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, fw_id=1))
    wf_meta = _get_metadata(snl)

    # add GGA insertion to DB
    spec = {'task_type': 'VASP db insertion', '_priority': 2}
    spec.update(_get_metadata(snl))
    fws.append(FireWork([VaspToDBTask()], spec, fw_id=2))
    connections[1] = 2

    # determine if GGA+U FW is needed
    mpvis = MaterialsProjectVaspInputSet()
    incar = mpvis.get_incar(snl.structure).to_dict

    if 'LDAU' in incar and incar['LDAU']:
        spec = {'task_type': 'GGA+U optimize structure (2x)', '_dupefinder': DupeFinderVasp().to_dict(), '_priority': 2}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask({'extension': '.relax2'}), SetupGGAUTask(), _get_custodian_task(spec)], spec, fw_id=3))
        connections[2] = 3

        spec = {'task_type': 'VASP db insertion', '_priority': 2}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask()], spec, fw_id=4))
        connections[3] = 4

        spec = {'task_type': 'GGA+U static', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), _get_custodian_task(spec)], spec, fw_id=5))
        connections[4] = 5

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask()], spec, fw_id=6))
        connections[5] = 6

        spec = {'task_type': 'GGA+U Uniform', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'uniform'}), _get_custodian_task(spec)], spec, fw_id=7))
        connections[6] = 7

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask({'parse_uniform': True})], spec, fw_id=8))
        connections[7] = 8

        spec = {'task_type': 'GGA+U band structure', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'line'}), _get_custodian_task(spec)], spec, fw_id=9))
        connections[8] = 9

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask({})], spec, fw_id=10))
        connections[9] = 10

    else:
        spec = {'task_type': 'GGA static', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), _get_custodian_task(spec)], spec, fw_id=3))
        connections[2] = 3

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask()], spec, fw_id=4))
        connections[3] = 4

        spec = {'task_type': 'GGA Uniform', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'uniform'}), _get_custodian_task(spec)], spec, fw_id=5))
        connections[4] = 5

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask({'parse_uniform': True})], spec, fw_id=6))
        connections[5] = 6

        spec = {'task_type': 'GGA band structure', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupNonSCFTask({'mode': 'line'}), _get_custodian_task(spec)], spec, fw_id=7))
        connections[6] = 7

        spec = {'task_type': 'VASP db insertion'}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspToDBTask({})], spec, fw_id=8))
        connections[7] = 8

        mpvis = MaterialsProjectGGAVaspInputSet()

    spec['vaspinputset_name'] = mpvis.__class__.__name__
    wf_meta['vaspinputset'] = mpvis.to_dict

    return Workflow(fws, connections, wf_meta)


def snl_to_wf_ggau(snl):
    fws = []
    connections = {}

    # add the root FW (GGA+U)
    spec = _snl_to_spec(snl, enforce_gga=False)
    tasks = [VaspWriterTask(), _get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, fw_id=1))
    wf_meta = _get_metadata(snl)

    # add GGA insertion to DB
    spec = {'task_type': 'VASP db insertion', '_priority': 2, '_category': 'VASP'}
    spec.update(_get_metadata(snl))
    fws.append(FireWork([VaspToDBTask()], spec, fw_id=2))
    connections[1] = 2
    mpvis = MaterialsProjectVaspInputSet()

    spec['vaspinputset_name'] = mpvis.__class__.__name__
    wf_meta['vaspinputset'] = mpvis.to_dict

    return Workflow(fws, connections, wf_meta)

if __name__ == '__main__':
    s1 = CifParser('test_wfs/Si.cif').get_structures()[0]
    s2 = CifParser('test_wfs/FeO.cif').get_structures()[0]

    snl1 = StructureNL(s1, "Anubhav Jain <ajain@lbl.gov>")
    snl1.data['_materialsproject'] = {'snl_id': 1, 'snlgroup_id': 1, 'snlgroupSG_id': 1}
    snl2 = StructureNL(s2, "Anubhav Jain <ajain@lbl.gov>")
    snl2.data['_materialsproject'] = {'snl_id': 2, 'snlgroup_id': 2, 'snlgroupSG_id': 2}

    snl_to_wf(snl1).to_file('test_wfs/wf_si_dupes.json', indent=4)
    snl_to_wf(snl2).to_file('test_wfs/wf_feo_dupes.json', indent=4)
