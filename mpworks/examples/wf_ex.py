from collections import defaultdict
from fireworks.core.firework import FireWork, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask
from pymatgen import Composition
from pymatgen.io.vaspio_set import MPGGAVaspInputSet

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Oct 03, 2013'

def get_name(structure, task_type):
    return get_slug(structure.formula + '--' + task_type)

def structure_to_wf(structure):
    """
    This method starts with a Structure object and creates a Workflow object
    The workflow has two steps - a structure relaxation and a static run
    :param structure:
    :return:
    """
    fws = []
    connections = defaultdict(list)

    # generate VASP input objects for the structure
    mpvis = MPGGAVaspInputSet(user_incar_settings={'NPAR': 2})
    incar = mpvis.get_incar(structure)
    poscar = mpvis.get_poscar(structure)
    kpoints = mpvis.get_kpoints(structure)
    potcar = mpvis.get_potcar(structure)

    # serialize the VASP output objects to the FW spec
    spec = {}
    spec['vasp'] = {}
    spec['vasp']['incar'] = incar.to_dict
    spec['vasp']['poscar'] = poscar.to_dict
    spec['vasp']['kpoints'] = kpoints.to_dict
    spec['vasp']['potcar'] = potcar.to_dict
    spec['vaspinputset_name'] = mpvis.__class__.__name__
    spec['task_type'] = 'GGA optimize structure (2x) example'

    # 1st FireWork - run GGA optimize structure
    # VaspWriterTask - write input files (INCAR, POSCAR, KPOINTS, POSCAR) based on spec
    # CustodianTaskEx - run VASP within a custodian

    jobs = VaspJob.double_relaxation_run(v_exe, gzipped=False)
        handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(),
                    NonConvergingErrorHandler()]

    tasks = [VaspWriterTask(), get_custodian_task(spec)]
    fws.append(FireWork(tasks, spec, name=get_name(structure, spec['task_type']), fw_id=1))

    # 2nd FireWork - insert previous run into DB
    spec = {'task_type': 'VASP db insertion example'}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_name(structure, spec['task_type']), fw_id=2))
    connections[1] = [2]

    # 3rd FireWork - static run.
    # VaspCopyTask - copy output from previous run to this directory
    # SetupStaticRunTask - override old parameters for static run
    # CustodianTaskEx - run VASP within a custodian
    spec = {'task_type': 'GGA static example'}
    fws.append(FireWork([VaspCopyTask({'use_CONTCAR': True, 'skip_CHGCAR': True}), SetupStaticRunTask(),get_custodian_task(spec)], spec, name=get_name(structure, spec['task_type']), fw_id=3))
    connections[2] = [3]

    # 4th FireWork - insert previous run into DB
    spec = {'task_type': 'VASP db insertion example'}
    fws.append(
        FireWork([VaspToDBTask()], spec, name=get_name(structure, spec['task_type']), fw_id=4))
    connections[3] = [4]

    return Workflow(fws, connections, name=get_slug(structure.formula))