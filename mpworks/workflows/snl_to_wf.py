from fireworks.core.firework import FireWork
from fireworks.core.workflow import Workflow
from mpworks.firetasks.custodian_task import CustodianTask
from mpworks.firetasks.vasp_io_tasks import VASPCopyTask, VASPWriterTask
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


def _snl_to_spec(snl, enforce_gga=True, inaccurate=False, dupecheck=True):
    spec = {}

    mpvis = MaterialsProjectGGAVaspInputSet() if enforce_gga else MaterialsProjectVaspInputSet()
    structure = snl.structure

    spec['vasp'] = {}
    spec['vasp']['incar'] = mpvis.get_incar(structure).to_dict
    spec['vasp']['poscar'] = mpvis.get_poscar(structure).to_dict
    spec['vasp']['kpoints'] = mpvis.get_kpoints(structure).to_dict
    spec['vasp']['potcar'] = mpvis.get_potcar(structure).to_dict

    spec['vaspinputset_name'] = mpvis.__class__.__name__

    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp']['incar'].get('LDAU', False) else 'GGA optimize structure (2x)'

    spec.update(_get_metadata(snl, verbose=True, dupecheck=dupecheck))

    #  override parameters for testing
    if inaccurate:
        spec['vasp']['incar']['EDIFF'] *= 10

    return spec


def _get_metadata(snl, verbose=False, dupecheck=True):
    md = {'run_tags': ['auto generation v1.0']}
    if verbose:
        md['snl'] = snl.to_dict

    if dupecheck:
        try:
            sd = snl.data['_materialsproject']
            md['snl_id'] = sd['snl_id']
            md['snlgroup_id'] = sd['snlgroup_id']
            md['snlgroupSG_id'] = sd['snlgroupsg_id']
        except:
            raise ValueError("SNL must contain snl_id, snlgroup_id, snlgroupSG_id in data['_materialsproject']['snl_id']")

    return md


def snl_to_wf(snl, inaccurate=False, dupecheck=True):
    # TODO: clean this up once we're out of testing mode
    # TODO: add WF metadata
    fws = []
    connections = {}
    # add the root FW (GGA)
    spec = _snl_to_spec(snl, enforce_gga=True, inaccurate=inaccurate, dupecheck=dupecheck)
    tasks = [VASPWriterTask(), CustodianTask()]
    fws.append(FireWork(tasks, spec, fw_id=-1))
    wf_meta = _get_metadata(snl, dupecheck=dupecheck)
    # determine if GGA+U FW is needed
    mpvis = MaterialsProjectVaspInputSet()
    incar = mpvis.get_incar(snl.structure).to_dict
    if 'LDAU' in incar and incar['LDAU']:
        spec = {'task_type': 'GGA+U optimize structure (2x)'}
        spec.update(_get_metadata(snl, dupecheck=dupecheck))
        fws.append(FireWork([VASPCopyTask({'extension': '.relax2'}), SetupGGAUTask(), CustodianTask()], spec, fw_id=-2))
        connections[-1] = -2

        spec = {'task_type': 'GGA+U static'}
        spec.update(_get_metadata(snl, dupecheck=dupecheck))
        fws.append(
            FireWork([VASPCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), CustodianTask()], spec, fw_id=-3))
        connections[-2] = -3

        spec = {'task_type': 'GGA+U DOS'}
        spec.update(_get_metadata(snl, dupecheck=dupecheck))
        fws.append(FireWork([VASPCopyTask(), SetupDOSRunTask(), CustodianTask()], spec, fw_id=-4))
        connections[-3] = -4
    else:
        spec = {'task_type': 'GGA static'}
        spec.update(_get_metadata(snl, dupecheck=dupecheck))
        fws.append(
            FireWork([VASPCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), CustodianTask()], spec, fw_id=-3))
        connections[-1] = -3

        spec = {'task_type': 'GGA DOS'}
        spec.update(_get_metadata(snl, dupecheck=dupecheck))
        fws.append(FireWork([VASPCopyTask(), SetupDOSRunTask(), CustodianTask()], spec, fw_id=-4))
        connections[-3] = -4

        mpvis = MaterialsProjectGGAVaspInputSet()

    spec['vaspinputset_name'] = mpvis.__class__.__name__
    wf_meta['vaspinputset'] = mpvis.to_dict

    return Workflow(fws, connections, wf_meta)


def snl_to_ggau_wf(snl):
    """
    This is a special workflow intended specifically for testing whether to run GGA+U runs without first running GGA>

    TODO: delete this (or at least comment it out) once we're done with tests.
    :param snl:
    :param testing:
    :return:
    """

    spec = _snl_to_spec(snl, enforce_gga=False, inaccurate=False, dupecheck=False)
    if not spec['vasp']['incar']['LDAU']:
        raise ValueError('This method is only intended for GGA+U structures!')
    tasks = [VASPWriterTask(), CustodianTask()]
    fw = FireWork(tasks, spec)
    return Workflow.from_FireWork(fw)


if __name__ == '__main__':
    s1 = CifParser('test_wfs/Si.cif').get_structures()[0]
    s2 = CifParser('test_wfs/FeO.cif').get_structures()[0]

    snl1 = StructureNL(s1, "Anubhav Jain <ajain@lbl.gov>")
    snl2 = StructureNL(s2, "Anubhav Jain <ajain@lbl.gov>")

    snl_to_wf(snl1, inaccurate=True, dupecheck=False).to_file('test_wfs/wf_si.json', indent=4)
    snl_to_wf(snl2, inaccurate=True, dupecheck=False).to_file('test_wfs/wf_feo.json', indent=4)

    snl_to_ggau_wf(snl2).to_file('test_wfs/wf_feo_GGAU.json', indent=4)