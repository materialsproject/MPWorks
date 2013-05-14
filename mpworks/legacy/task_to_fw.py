import datetime
from fireworks.core.firework import FireTaskBase, FireWork, Launch, FWAction, Workflow
from fireworks.utilities.fw_serializers import FWSerializable
from fireworks.utilities.fw_utilities import get_slug
from mpworks.snl_utils.mpsnl import get_meta_from_structure
from pymatgen import Composition, Structure


__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 14, 2013'


class DummyLegacyTask(FireTaskBase, FWSerializable):
    _fw_name = "Dummy Legacy Task"

    def run_task(self, fw_spec):
        pass


def task_dict_to_wf(task_dict, launchpad):
    fw_id = launchpad.get_new_fw_id()
    l_id = launchpad.get_new_launch_id()

    spec = {'task_type': task_dict['task_type'], 'run_tags': task_dict['run_tags'],
            'vaspinputset_name': None, 'vasp': None, 'mpsnl': task_dict['snl'],
            'snlgroup_id': task_dict['snlgroup_id']}
    tasks = [DummyLegacyTask()]

    launch_dir = task_dict['dir_name_full']

    stored_data = {'error_list': []}
    update_spec = {'prev_vasp_dir': task_dict['dir_name'],
                   'prev_task_type': spec['task_type'],
                   'mpsnl': spec['mpsnl'], 'snlgroup_id': spec['snlgroup_id'],
                   'run_tags': spec['run_tags']}

    fwaction = FWAction(stored_data=stored_data, update_spec=update_spec)

    complete_date = datetime.datetime.strptime(task_dict['completed_at'], "%Y-%m-%d %H:%M:%S")
    state_history = [{"created_on": complete_date, 'state': 'COMPLETED'}]

    launches = [Launch('COMPLETED', launch_dir, fworker=None, host=None, ip=None, action=fwaction,
                       state_history=state_history, launch_id=l_id, fw_id=fw_id)]

    f = Composition.from_formula(task_dict['pretty_formula']).alphabetical_formula


    fw = FireWork(tasks, spec, name=get_slug(f + '--' + spec['task_type']), launches=launches, state='COMPLETED', created_on=None,
                 fw_id=fw_id)

    wf_meta = get_meta_from_structure(Structure.from_dict(task_dict['snl']))
    wf_meta['run_version'] = 'preproduction (0)'

    wf = Workflow.from_FireWork(fw, name=f, metadata=wf_meta)

    launchpad.add_wf(wf, reassign_all=False)

    print 'ADDED', fw_id
    # return fw_id
    return fw_id