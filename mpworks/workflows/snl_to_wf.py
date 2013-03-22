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

import numpy as np


def _snl_to_spec(snl, enforce_gga=True, testing=False):
    spec = {}

    mpvis = MaterialsProjectGGAVaspInputSet() if enforce_gga else MaterialsProjectVaspInputSet()
    structure = snl.structure

    spec['vasp'] = {}
    spec['vasp']['incar'] = mpvis.get_incar(structure).to_dict
    spec['vasp']['poscar'] = mpvis.get_poscar(structure).to_dict
    spec['vasp']['kpoints'] = mpvis.get_kpoints(structure).to_dict
    spec['vasp']['potcar'] = mpvis.get_potcar(structure).to_dict

    spec['vaspinputset_name'] = mpvis.__class__.__name__
    # TODO: put entire vaspinputset.to_dict in the WF metadata?

    spec['task_type'] = 'GGA+U optimize structure (2x)' if spec['vasp']['incar'].get('LDAU', False) else 'GGA optimize structure (2x)'
    spec['snl'] = snl.to_dict
    spec['snl_id'] = -1
    spec['snl_group'] = -2
    spec['snl_strictgroup'] = -2
    spec['tags'] = ['auto_generation_v1.0']

    #  override parameters for testing
    if testing:
        spec['vasp']['incar']['EDIFF'] *= 10

    return spec


def snl_to_wf(snl, testing=False):
    # TODO: clean this up once we're out of testing mode
    # TODO: add WF metadata
    fws = []
    connections = {}

    # add the root FW (GGA)
    spec = _snl_to_spec(snl, enforce_gga=True, testing=testing)
    tasks = [VASPWriterTask(), CustodianTask()]
    fws.append(FireWork(tasks, spec, fw_id=-1))

    # determine if GGA+U FW is needed
    mpvis = MaterialsProjectVaspInputSet()
    incar = mpvis.get_incar(snl.structure).to_dict
    if 'LDAU' in incar and incar['LDAU']:
        spec = {'task_type': 'GGA+U optimize structure (2x)'}  # TODO: add more spec keys? SNL, etc?

        fws.append(FireWork([VASPCopyTask({'extension': '.relax2'}), SetupGGAUTask(), CustodianTask()], spec, fw_id=-2))
        connections[-1] = -2
        spec = {'task_type': 'GGA+U static'}  # TODO: add more spec keys? SNL, etc?
        fws.append(
            FireWork([VASPCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), CustodianTask()], spec, fw_id=-3))
        connections[-2] = -3
        spec = {'task_type': 'GGA+U DOS'}  # TODO: add more spec keys? SNL, etc?
        fws.append(FireWork([VASPCopyTask(), SetupDOSRunTask(), CustodianTask()], spec, fw_id=-4))
        connections[-3] = -4
    else:
        spec = {'task_type': 'GGA static'}  # TODO: add more spec keys? SNL, etc?
        fws.append(
            FireWork([VASPCopyTask({'extension': '.relax2'}), SetupStaticRunTask(), CustodianTask()], spec, fw_id=-3))
        connections[-1] = -3
        spec = {'task_type': 'GGA DOS'}  # TODO: add more spec keys? SNL, etc?
        fws.append(FireWork([VASPCopyTask(), SetupDOSRunTask(), CustodianTask()], spec, fw_id=-4))
        connections[-3] = -4

    # TODO: check if gap > 1 eV before adding static run

    return Workflow(fws, connections)


def snl_to_ggau_wf(snl):
    """
    This is a special workflow intended specifically for testing whether to run GGA+U runs without first running GGA>

    TODO: delete this (or at least comment it out) once we're done with tests.
    :param snl:
    :param testing:
    :return:
    """

    spec = _snl_to_spec(snl, enforce_gga=False, testing=False)
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

    snl_to_wf(snl1, testing=True).to_file('test_wfs/wf_si.json', indent=4)
    snl_to_wf(snl2, testing=True).to_file('test_wfs/wf_feo.json', indent=4)

    snl_to_ggau_wf(snl2).to_file('test_wfs/wf_feo_GGAU.json', indent=4)