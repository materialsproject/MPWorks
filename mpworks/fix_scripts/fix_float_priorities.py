from _socket import timeout
import os
from fireworks.core.launchpad import LaunchPad

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Nov 26, 2013'


if __name__ == '__main__':
    module_dir = os.path.dirname(os.path.abspath(__file__))
    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)


    for fw in lpdb.fireworks.find({"spec._tasks.1.max_errors":{"$type": 1}}, {"fw_id": 1, "state": 1, "spec._tasks": 1}, timeout=False):
        print fw['fw_id'], fw['state']
        lpdb.fireworks.find_and_modify({"fw_id": fw['fw_id']}, {"$set": {"spec._tasks.1.max_errors": int(5)}})
        if fw['state'] == 'FIZZLED':
            lpdb.rerun_fw(fw['fw_id'])