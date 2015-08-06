
__author__ = 'Bharat Medasani'

from pymatgen.io.vaspio import Poscar
from pymatgen import Composition

from fireworks.core.firework import Firework, Workflow
from fireworks.utilities.fw_utilities import get_slug

from mpworks.firetasks.custodian_task import get_custodian_task
from mpworks.firetasks.snl_tasks import AddSNLTask
from mpworks.firetasks.vasp_io_tasks import VaspCopyTask, VaspWriterTask, \
    VaspToDBTask
from mpworks.firetasks.vasp_setup_tasks import SetupGGAUTask
from mpworks.snl_utils.mpsnl import get_meta_from_structure, MPStructureNL
from mpworks.workflows.wf_settings import QA_DB, QA_VASP, QA_CONTROL
#from mpworks.workflows import snl_to_wf
from mpworks.firetasks.im_defect_tasks import update_spec_defect_supercells, \
        update_spec_bulk_supercell
from mpworks.firetasks.im_defect_tasks import SetupDefectSupercellStructTask


def snl_to_wf_im_defects(snl, parameters):
    """
    Generates Intermetallic defect workflow for the submitted SNL object.
    """
    # parameters["user_vasp_settings"] specifies user defined incar/kpoints parameters
    fws = []
    connections = {}
    parameters = parameters if parameters else {}

    snl_priority = parameters.get('priority', 1)
    priority = snl_priority * 2  # once we start a job, keep going!

    f = Composition(snl.structure.composition.reduced_formula).alphabetical_formula

    # add the SNL to the SNL DB and figure out duplicate group
    #tasks = [AddSNLTask()]
    #spec = {'task_type': 'Add to SNL database', 'snl': snl.as_dict(), '_queueadapter': QA_DB, '_priority': snl_priority}
    #if 'snlgroup_id' in parameters and isinstance(snl, MPStructureNL):
    #    spec['force_mpsnl'] = snl.as_dict()
    #    spec['force_snlgroup_id'] = parameters['snlgroup_id']
    #    del spec['snl']
    #fws.append(Firework(tasks, spec, name=get_slug(f + '--' + spec['task_type']), fw_id=0))
    #connections[0] = [1]

    parameters["exact_structure"] = True

    spec = {'task_type': 'Setup Defect Supercell Struct Task', 'snl':  snl.as_dict(),
            '_priority': priority, '_queueadapter': QA_CONTROL, '_parameters': parameters}
    fws.append(
            Firework([SetupDefectSupercellStructTask()], spec, name=get_slug(f + '--' + spec['task_type']),
                     fw_id=0))
    #connections[1] = [2]

    wf_meta = get_meta_from_structure(snl.structure)
    wf_meta['run_version'] = 'May 2013 (1)'

    if '_materialsproject' in snl.data and 'submission_id' in snl.data['_materialsproject']:
        wf_meta['submission_id'] = snl.data['_materialsproject']['submission_id']

    return Workflow(fws, connections, name=Composition(
        snl.structure.composition.reduced_formula).alphabetical_formula, metadata=wf_meta)
