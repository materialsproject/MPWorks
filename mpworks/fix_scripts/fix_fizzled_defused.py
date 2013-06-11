import os
from fireworks.core.launchpad import LaunchPad

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jun 05, 2013'


def restart_fizzled():
    module_dir = os.path.dirname(os.path.abspath(__file__))
    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)

    for fw in lpdb.fireworks.find({"state": "FIZZLED"}, {"fw_id": 1, "spec.task_type": 1}):
        fw_id = fw['fw_id']
        task_type = fw['spec']['task_type']
        restart_id = fw_id
        if 'VASP db insertion' in task_type:
            restart_id = fw_id - 1
        elif 'Controller' in task_type:
            restart_id = fw_id - 2

        lpdb.rerun_fw(restart_id)


if __name__ == '__main__':
    restart_fizzled()