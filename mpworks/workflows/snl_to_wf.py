from custodian.vasp.jobs import VaspJob
from custodian.vasp.handlers import VaspErrorHandler, PoscarErrorHandler
from fireworks.core.firework import FireWork
from fireworks.core.workflow import Workflow
from mpworks.dupefinders.dupefinder_vasp import DupeFinderVasp
from mpworks.firetasks.custodian_task import VaspCustodianTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask, SetupStaticRunTask, SetupDOSRunTask
from pymatgen.io.cifio import CifParser
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet, MaterialsProjectGGAVaspInputSet
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'


def _get_custodian_task(spec):
    task_type = spec['task_type']
    v_exe = 'VASP_EXE'  # will be transformed to vasp executable on the node
    if 'static' in task_type or 'DOS' in task_type:
        jobs = [VaspJob(v_exe)]
    elif 'optimize structure (2x)' in task_type:
        jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
    else:
        raise ValueError('Unrecognized task type!')

    handlers = [VaspErrorHandler(), PoscarErrorHandler()]
    params = {'jobs': [j.to_dict for j in jobs], 'handlers': [h.to_dict for h in handlers], 'max_errors': 10}

    return VaspCustodianTask(params)


def _snl_to_spec(snl, enforce_gga=True, inaccurate=False):
    spec = {}

    mpvis = MaterialsProjectGGAVaspInputSet() if enforce_gga else MaterialsProjectVaspInputSet()
    structure = snl.structure

    spec['vasp'] = {}
    spec['vasp']['incar'] = mpvis.get_incar(structure).to_dict
    spec['vasp']['poscar'] = mpvis.get_poscar(structure).to_dict
    spec['vasp']['kpoints'] = mpvis.get_kpoints(structure).to_dict
    spec['vasp']['potcar'] = mpvis.get_potcar(structure).to_dict
    spec['_dupefinder'] = DupeFinderVasp().to_dict()
    spec['vaspinputset_name'] = mpvis.__class__.__name__

    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp']['incar'].get('LDAU', False) else 'GGA optimize structure (2x)'

    spec.update(_get_metadata(snl, verbose=True))

    #  override parameters for testing
    if inaccurate:
        spec['vasp']['incar']['EDIFF'] *= 10

    return spec


def _get_metadata(snl, verbose=False):
    md = {'run_tags': ['auto generation v1.0']}
    if verbose:
        md['snl'] = snl.to_dict

    try:
        sd = snl.data['_materialsproject']
        md['snl_id'] = sd['snl_id']
        md['snlgroup_id'] = sd['snlgroup_id']
        md['snlgroupSG_id'] = sd['snlgroupSG_id']
    except:
        raise ValueError("SNL must contain snl_id, snlgroup_id, snlgroupSG_id in data['_materialsproject']['snl_id']")

    return md


def snl_to_wf(snl, inaccurate=False):
    # TODO: clean this up once we're out of testing mode
    # TODO: add WF metadata
    fws = []
    connections = {}
    # add the root FW (GGA)
    spec = _snl_to_spec(snl, enforce_gga=True, inaccurate=inaccurate)
    tasks = [VaspWriterTask(), _get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, fw_id=-1))
    wf_meta = _get_metadata(snl)

    # determine if GGA+U FW is needed
    mpvis = MaterialsProjectVaspInputSet()
    incar = mpvis.get_incar(snl.structure).to_dict
    if 'LDAU' in incar and incar['LDAU']:
        spec = {'task_type': 'GGA+U optimize structure (2x)', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask({'extension': '.relax2'}), SetupGGAUTask(), _get_custodian_task(spec)], spec, fw_id=-2))
        connections[-1] = -2

        spec = {'task_type': 'GGA+U static', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), _get_custodian_task(spec)], spec, fw_id=-3))
        connections[-2] = -3

        spec = {'task_type': 'GGA+U DOS', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupDOSRunTask(), _get_custodian_task(spec)], spec, fw_id=-4))
        connections[-3] = -4
    else:
        spec = {'task_type': 'VASP db insertion'}
        fws.append(
            FireWork([VaspToDBTask()], spec, fw_id=-2))
        connections[-1] = -2

        spec = {'task_type': 'GGA static', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(
            FireWork([VaspCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), _get_custodian_task(spec)], spec, fw_id=-3))
        connections[-2] = -3

        spec = {'task_type': 'GGA DOS', '_dupefinder': DupeFinderVasp().to_dict()}
        spec.update(_get_metadata(snl))
        fws.append(FireWork([VaspCopyTask(), SetupDOSRunTask(), _get_custodian_task(spec)], spec, fw_id=-4))
        connections[-3] = -4

        mpvis = MaterialsProjectGGAVaspInputSet()

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

    snl_to_wf(snl1, inaccurate=True).to_file('test_wfs/wf_si_dupes.json', indent=4)
    snl_to_wf(snl2, inaccurate=True).to_file('test_wfs/wf_feo_dupes.json', indent=4)
