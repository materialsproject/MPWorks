from collections import defaultdict
from pprint import pprint
from custodian.vasp.handlers import VaspErrorHandler, FrozenJobErrorHandler, MeshSymmetryErrorHandler, NonConvergingErrorHandler
from custodian.vasp.jobs import VaspJob
from fireworks.core.firework import Firework, Workflow
from fireworks.utilities.fw_utilities import get_slug
from mpworks.examples.firetasks_ex import VaspCustodianTaskEx, VaspToDBTaskEx
from mpworks.firetasks.vasp_io_tasks import VaspWriterTask, VaspCopyTask
from mpworks.firetasks.vasp_setup_tasks import SetupStaticRunTask
from pymatgen import Composition, Lattice
from pymatgen.core.structure import Structure
from pymatgen.io.vasp.sets import MPRelaxSet

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
    fws = []  # list of FireWorks to run
    connections = defaultdict(list)  # dependencies between FireWorks

    # generate VASP input objects for 1st VASP run - this is put in the FW spec
    mpvis = MPRelaxSet(structure, user_incar_settings={'NPAR': 2, 
                                                       "GGAU":False})
    incar = mpvis.incar
    poscar = mpvis.poscar
    kpoints = mpvis.poscar
    potcar = mpvis.poscar

    # serialize the VASP input objects to the FW spec
    spec = {}
    spec['vasp'] = {}
    spec['vasp']['incar'] = incar.as_dict()
    spec['vasp']['poscar'] = poscar.as_dict()
    spec['vasp']['kpoints'] = kpoints.as_dict()
    spec['vasp']['potcar'] = potcar.as_dict()
    spec['vaspinputset_name'] = mpvis.__class__.__name__
    spec['task_type'] = 'GGA optimize structure (2x) example'

    # set up the custodian that we want to run
    jobs = VaspJob.double_relaxation_run('')
    for j in jobs: # turn off auto npar, it doesn't work for >1 node
            j.auto_npar = False
    handlers = [VaspErrorHandler(), FrozenJobErrorHandler(), MeshSymmetryErrorHandler(),
                    NonConvergingErrorHandler()]
    c_params = {'jobs': [j.as_dict() for j in jobs], 'handlers': [h.as_dict() for h in handlers], 'max_errors': 5}
    custodiantask = VaspCustodianTaskEx(c_params)

    # 1st Firework - run GGA optimize structure
    # VaspWriterTask - write input files (INCAR, POSCAR, KPOINTS, POSCAR) based on spec
    # CustodianTaskEx - run VASP within a custodian
    tasks = [VaspWriterTask(), custodiantask]
    fws.append(Firework(tasks, spec, name=get_name(structure, spec['task_type']), fw_id=1))

    # 2nd Firework - insert previous run into DB
    spec = {'task_type': 'VASP db insertion example'}
    fws.append(
        Firework([VaspToDBTaskEx()], spec, name=get_name(structure, spec['task_type']), fw_id=2))
    connections[1] = [2]

    # 3rd Firework - static run.
    # VaspCopyTask - copy output from previous run to this directory
    # SetupStaticRunTask - override old parameters for static run
    # CustodianTaskEx - run VASP within a custodian
    spec = {'task_type': 'GGA static example'}
    copytask = VaspCopyTask({'use_CONTCAR': True, 'skip_CHGCAR': True})
    setuptask = SetupStaticRunTask()
    custodiantask = VaspCustodianTaskEx({'jobs': [VaspJob('', auto_npar=False).as_dict()], 'handlers': [h.as_dict() for h in handlers], 'max_errors': 5})
    fws.append(Firework([copytask, setuptask, custodiantask], spec, name=get_name(structure, spec['task_type']), fw_id=3))
    connections[2] = [3]

    # 4th Firework - insert previous run into DB
    spec = {'task_type': 'VASP db insertion example'}
    fws.append(
        Firework([VaspToDBTaskEx()], spec, name=get_name(structure, spec['task_type']), fw_id=4))
    connections[3] = [4]

    return Workflow(fws, connections, name=get_slug(structure.formula))

if __name__ == '__main__':
    l = Lattice.from_parameters(3.866, 3.866, 3.866, 60, 60, 60)
    s = Structure(l, ['Si', 'Si'], [[0.125,0.125,0.125], [0.875,0.875,0.875]])

    my_wf = structure_to_wf(s)
    pprint(my_wf.to_dict(), indent=2)
    my_wf.to_file("Si_wf.json")
