from fireworks.core.firework import FireWork
from fireworks.core.workflow import Workflow
from mpworks.firetasks.custodian_task import CustodianTask
from mpworks.firetasks.vasp_tasks import VASPWriterTask, VASPCopyTask, SetupGGAUTask
from pymatgen.core.structure import Structure
from pymatgen.io.vaspio_set import MaterialsProjectVaspInputSet, MaterialsProjectGGAVaspInputSet, MITVaspInputSet
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 15, 2013'

import numpy as np


def _snl_to_spec(snl):
    spec = {}

    mpvis = MaterialsProjectGGAVaspInputSet()
    structure = snl.structure

    spec['vasp'] = {}
    spec['vasp']['incar'] = mpvis.get_incar(structure).to_dict
    spec['vasp']['poscar'] = mpvis.get_poscar(structure).to_dict
    spec['vasp']['kpoints'] = mpvis.get_kpoints(structure).to_dict
    spec['vasp']['potcar'] = mpvis.get_potcar(structure).to_dict

    spec['vaspinputset_name'] = mpvis.__class__.__name__
    # TODO: put entire vaspinputset.to_dict in the WF metadata?
    spec['task_type'] = 'GGA optimize structure (2x)'
    spec['snl'] = snl.to_dict
    spec['snl_id'] = -1
    spec['snl_group'] = -2
    spec['snl_strictgroup'] = -2
    spec['tags'] = ['auto_generation_v1.0']

    return spec


def _snl_to_fw(snl, fw_id=-1):

    spec = _snl_to_spec(snl)
    tasks = [VASPWriterTask(), CustodianTask()]
    return FireWork(tasks, spec, fw_id=fw_id)


def snl_to_wf(snl):
    # TODO: add WF metadata
    fws = []
    connections = {}

    fws.append(_snl_to_fw(snl, fw_id=-1))  # add the GGA FireWork to the workflow

    # determine if GGA+U FW is needed
    mpvis = MaterialsProjectVaspInputSet()
    incar = mpvis.get_incar(snl.structure).to_dict
    if 'LDAU' in incar and incar['LDAU']:
        spec = {}
        spec['task_type'] = 'GGA+U optimize structure (2x)'
        # TODO: add more specs
        incar_updates = { k: incar[k] for k in incar.keys() if 'LDAU' in k}  # LDAU values to use
        fws.append(FireWork([VASPCopyTask(), SetupGGAUTask(incar_updates), CustodianTask()], spec, fw_id=-2))
        connections[-1] = -2

    return Workflow(fws, connections)



if __name__ == '__main__':
    s = Structure(np.eye(3, 3) * 3, ["Si", "Si"], [[0, 0, 0], [0.25, 0.25, 0.25]])
    snl1 = StructureNL(s, "Anubhav Jain <ajain@lbl.gov>")

    s2 = Structure(np.eye(3, 3) * 3, ["Fe", "O"], [[0, 0, 0], [0.25, 0.25, 0.25]])
    snl2 = StructureNL(s2, "Anubhav Jain <ajain@lbl.gov>")

    snl_to_wf(snl1).to_file('test_wfs/wf_si.json', indent=4)
    snl_to_wf(snl2).to_file('test_wfs/wf_feo.json', indent=4)
